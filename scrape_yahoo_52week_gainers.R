#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
  library(dplyr)
  library(purrr)
  library(rvest)
  library(xml2)
  library(readr)
  library(stringr)
  library(tibble)
  library(lubridate)
})

message("Starting scrape_yahoo_52week_gainers.R")

get_env_or_stop <- function(x) {
  val <- Sys.getenv(x, unset = "")
  if (!nzchar(val)) stop("Missing env var: ", x, call. = FALSE)
  val
}

db_connect_supabase <- function() {
  DBI::dbConnect(
    RPostgres::Postgres(),
    host     = get_env_or_stop("SUPABASE_HOST"),
    port     = as.integer(get_env_or_stop("SUPABASE_PORT")),
    dbname   = get_env_or_stop("SUPABASE_DB"),
    user     = get_env_or_stop("SUPABASE_USER"),
    password = get_env_or_stop("SUPABASE_PASSWORD"),
    sslmode  = "require"
  )
}

download_page_html <- function(url, dest_file) {
  curl_bin <- Sys.which("curl")
  if (!nzchar(curl_bin)) curl_bin <- "curl"
  
  status <- tryCatch(
    system2(
      curl_bin,
      args = c(
        "-L",
        "-A", "Mozilla/5.0",
        "-o", dest_file,
        url
      ),
      stdout = TRUE,
      stderr = TRUE
    ),
    error = function(e) e
  )
  
  !inherits(status, "error") && file.exists(dest_file)
}

parse_yahoo_num <- function(x) {
  x <- str_trim(as.character(x))
  
  multiplier <- case_when(
    str_detect(x, "[Kk]$") ~ 1e3,
    str_detect(x, "[Mm]$") ~ 1e6,
    str_detect(x, "[Bb]$") ~ 1e9,
    str_detect(x, "[Tt]$") ~ 1e12,
    TRUE ~ 1
  )
  
  number_part <- x %>%
    str_remove("[KkMmBbTt]$") %>%
    parse_number()
  
  number_part * multiplier
}

scrape_yahoo_page <- function(start_value,
                              page_size = 25,
                              dest_folder = tempdir(),
                              base_url = "https://finance.yahoo.com/markets/stocks/52-week-gainers/") {
  url <- paste0(base_url, "?start=", start_value, "&count=", page_size)

  html_file <- file.path(
    dest_folder,
    paste0("yahoo_52_week_gainers_", start_value, ".html")
  )

  ok <- download_page_html(url, html_file)
  if (!ok) {
    message("Download failed for start=", start_value)
    return(NULL)
  }

  page <- tryCatch(read_html(html_file), error = function(e) NULL)
  if (is.null(page)) {
    message("Could not parse HTML for start=", start_value)
    return(NULL)
  }

  tables <- page %>%
    html_elements("table") %>%
    html_table(fill = TRUE)

  if (length(tables) == 0) {
    message("No tables found for start=", start_value)
    return(NULL)
  }

  out <- as_tibble(tables[[1]], .name_repair = "minimal")

  nm <- names(out)
  blank_idx <- which(is.na(nm) | nm == "")

  if (length(blank_idx) > 0) {
    nm[blank_idx] <- paste0("Chart", seq_along(blank_idx))
    names(out) <- nm
  }

  out %>%
    mutate(
      start = start_value,
      source_url = url
    )
}

clean_yahoo_table <- function(all_data) {
  run_date <- today(tzone = "America/New_York")
  
  all_data %>%
    mutate(
      Price_raw = Price,
      Price = str_extract(Price_raw, "^[0-9,.]+"),
      Price = parse_number(Price),
      `Change` = parse_number(`Change`),
      `Change %` = parse_number(`Change %`),
      Volume = parse_yahoo_num(Volume),
      `Avg Vol (3M)` = parse_yahoo_num(`Avg Vol (3M)`),
      `Market Cap` = parse_yahoo_num(`Market Cap`),
      `52 Wk Change %` = parse_number(`52 Wk Change %`),
      `P/E Ratio (TTM)` = na_if(`P/E Ratio (TTM)`, "--"),
      `P/E Ratio (TTM)` = parse_number(`P/E Ratio (TTM)`),
      `52 Wk Range` = str_squish(`52 Wk Range`)
    ) %>%
    separate(
      `52 Wk Range`,
      into = c("wk52_low", "wk52_high"),
      sep = "\\s+",
      fill = "right",
      remove = TRUE
    ) %>%
    mutate(
      wk52_low  = as.numeric(wk52_low),
      wk52_high = as.numeric(wk52_high)
    ) %>%
    select(-any_of("Chart")) %>%
    mutate(date = as.Date(run_date))
}

create_target_table <- function(con) {
  DBI::dbExecute(con, '
    CREATE TABLE IF NOT EXISTS public.yahoo_52_week_gainers (
      "Symbol" text NOT NULL,
      "Name" text,
      "Price" double precision,
      "Change" double precision,
      "Change %" double precision,
      "Volume" double precision,
      "Avg Vol (3M)" double precision,
      "Market Cap" double precision,
      "P/E Ratio (TTM)" double precision,
      "52 Wk Change %" double precision,
      "wk52_low" double precision,
      "wk52_high" double precision,
      "start" double precision,
      "source_url" text,
      "Price_raw" text,
      "date" date NOT NULL,
      PRIMARY KEY ("Symbol", "date")
    );
  ')
}

upsert_yahoo_gainers <- function(con, df) {
  upload_df <- df %>%
    transmute(
      Symbol            = as.character(Symbol),
      Name              = as.character(Name),
      Price             = as.numeric(Price),
      Change            = as.numeric(Change),
      `Change %`        = as.numeric(`Change %`),
      Volume            = as.numeric(Volume),
      `Avg Vol (3M)`    = as.numeric(`Avg Vol (3M)`),
      `Market Cap`      = as.numeric(`Market Cap`),
      `P/E Ratio (TTM)` = as.numeric(`P/E Ratio (TTM)`),
      `52 Wk Change %`  = as.numeric(`52 Wk Change %`),
      wk52_low          = as.numeric(wk52_low),
      wk52_high         = as.numeric(wk52_high),
      start             = as.numeric(start),
      source_url        = as.character(source_url),
      Price_raw         = as.character(Price_raw),
      date              = as.Date(date)
    ) %>%
    distinct(Symbol, date, .keep_all = TRUE)
  
  if (nrow(upload_df) == 0) {
    stop("upload_df has 0 rows, nothing to upload.", call. = FALSE)
  }
  
  DBI::dbWithTransaction(con, {
    DBI::dbWriteTable(
      con,
      "tmp_yahoo_52_week_gainers",
      upload_df,
      temporary = TRUE,
      overwrite = TRUE
    )
    
    DBI::dbExecute(con, '
      INSERT INTO public.yahoo_52_week_gainers AS t
        ("Symbol", "Name", "Price", "Change", "Change %", "Volume",
         "Avg Vol (3M)", "Market Cap", "P/E Ratio (TTM)", "52 Wk Change %",
         "wk52_low", "wk52_high", "start", "source_url", "Price_raw", "date")
      SELECT
        "Symbol", "Name", "Price", "Change", "Change %", "Volume",
        "Avg Vol (3M)", "Market Cap", "P/E Ratio (TTM)", "52 Wk Change %",
        "wk52_low", "wk52_high", "start", "source_url", "Price_raw", "date"
      FROM tmp_yahoo_52_week_gainers
      ON CONFLICT ("Symbol", "date") DO UPDATE
      SET
        "Name"            = EXCLUDED."Name",
        "Price"           = EXCLUDED."Price",
        "Change"          = EXCLUDED."Change",
        "Change %"        = EXCLUDED."Change %",
        "Volume"          = EXCLUDED."Volume",
        "Avg Vol (3M)"    = EXCLUDED."Avg Vol (3M)",
        "Market Cap"      = EXCLUDED."Market Cap",
        "P/E Ratio (TTM)" = EXCLUDED."P/E Ratio (TTM)",
        "52 Wk Change %"  = EXCLUDED."52 Wk Change %",
        "wk52_low"        = EXCLUDED."wk52_low",
        "wk52_high"       = EXCLUDED."wk52_high",
        "start"           = EXCLUDED."start",
        "source_url"      = EXCLUDED."source_url",
        "Price_raw"       = EXCLUDED."Price_raw";
    ')
  })
  
  count_df <- DBI::dbGetQuery(
    con,
    'SELECT "date", COUNT(*) AS n
     FROM public.yahoo_52_week_gainers
     GROUP BY "date"
     ORDER BY "date" DESC
     LIMIT 5'
  )
  
  print(count_df)
}

main <- function() {
  base_url    <- "https://finance.yahoo.com/markets/stocks/52-week-gainers/"
  dest_folder <- tempdir()
  total_rows  <- 150
  page_size   <- 25
  starts      <- seq(0, total_rows - page_size, by = page_size)
  
  all_data <- purrr::map_dfr(
    starts,
    scrape_yahoo_page,
    page_size = page_size,
    dest_folder = dest_folder,
    base_url = base_url
  )
  
  if (nrow(all_data) == 0) {
    stop("Scraper returned 0 rows.", call. = FALSE)
  }
  
  message("Raw rows scraped: ", nrow(all_data))
  
  all_data_clean <- clean_yahoo_table(all_data)
  
  message("Clean rows: ", nrow(all_data_clean))
  print(all_data_clean %>% count(date))
  
  con <- db_connect_supabase()
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  
  DBI::dbGetQuery(con, "select 'connected' as status, now();") |>
    print()
  
  create_target_table(con)
  upsert_yahoo_gainers(con, all_data_clean)
  
  message("Done.")
}

main()
