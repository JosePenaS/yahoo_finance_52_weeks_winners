#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
  library(dplyr)
  library(purrr)
  library(tibble)
  library(jsonlite)
  library(zoo)
})

message("Starting update_yahoo_52wk_trades.R")

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

empty_results_tbl <- function() {
  tibble(
    symbol      = character(),
    name        = character(),
    signal_date = as.Date(character()),
    entry_date  = as.Date(character()),
    ma10        = numeric(),
    ma20        = numeric(),
    ma100       = numeric(),
    open        = numeric(),
    high        = numeric(),
    low         = numeric(),
    close       = numeric(),
    entry_price = numeric(),
    stop_price  = numeric(),
    stop_hit    = logical(),
    exit_price  = numeric(),
    exit_reason = character(),
    hold_days   = integer(),
    return_pct  = numeric()
  )
}

create_target_table <- function(con) {
  DBI::dbExecute(con, '
    CREATE TABLE IF NOT EXISTS public.yahoo_52wk_trades (
      symbol text NOT NULL,
      name text,
      signal_date date NOT NULL,
      entry_date date,
      ma10 double precision,
      ma20 double precision,
      ma100 double precision,
      open double precision,
      high double precision,
      low double precision,
      close double precision,
      entry_price double precision,
      stop_price double precision,
      stop_hit boolean,
      exit_price double precision,
      exit_reason text,
      hold_days integer,
      return_pct double precision,
      PRIMARY KEY (symbol, signal_date)
    );
  ')
}

get_latest_usable_signal_date <- function(con) {
  out <- DBI::dbGetQuery(con, '
    SELECT DISTINCT "date"
    FROM public.yahoo_52_week_gainers
    ORDER BY "date" DESC
    OFFSET 1
    LIMIT 1
  ')
  
  if (nrow(out) == 0 || is.na(out$date[1])) {
    return(as.Date(NA))
  }
  
  as.Date(out$date[1])
}

get_signal_universe <- function(con, signal_date) {
  q <- sprintf(
    'SELECT * FROM public.yahoo_52_week_gainers WHERE "date" = %s',
    DBI::dbQuoteLiteral(con, as.character(signal_date))
  )
  
  DBI::dbGetQuery(con, q) %>%
    as_tibble()
}

download_yahoo_chart <- function(symbol, from, to, dest_folder = tempdir()) {
  period1 <- as.integer(as.POSIXct(from, tz = "UTC"))
  period2 <- as.integer(as.POSIXct(to,   tz = "UTC"))
  
  url <- paste0(
    "https://query1.finance.yahoo.com/v8/finance/chart/",
    symbol,
    "?period1=", period1,
    "&period2=", period2,
    "&interval=1d&includePrePost=false&events=div%2Csplits"
  )
  
  json_file <- file.path(dest_folder, paste0(symbol, "_chart.json"))
  curl_bin  <- Sys.which("curl")
  if (!nzchar(curl_bin)) curl_bin <- "curl"
  
  status <- tryCatch(
    system2(
      curl_bin,
      args = c(
        "-L",
        "-A", "Mozilla/5.0",
        "-o", json_file,
        url
      ),
      stdout = TRUE,
      stderr = TRUE
    ),
    error = function(e) e
  )
  
  if (!file.exists(json_file) || inherits(status, "error")) {
    message("Failed download: ", symbol)
    return(NULL)
  }
  
  txt <- tryCatch(
    paste(readLines(json_file, warn = FALSE, encoding = "UTF-8"), collapse = "\n"),
    error = function(e) NULL
  )
  if (is.null(txt) || !nzchar(txt)) return(NULL)
  
  obj <- tryCatch(fromJSON(txt, simplifyDataFrame = FALSE), error = function(e) NULL)
  if (is.null(obj)) return(NULL)
  
  result <- obj$chart$result[[1]]
  if (is.null(result) || is.null(result$timestamp)) return(NULL)
  
  q <- result$indicators$quote[[1]]
  if (is.null(q)) return(NULL)
  
  tibble(
    symbol = symbol,
    date   = as.Date(as.POSIXct(result$timestamp, origin = "1970-01-01", tz = "UTC")),
    open   = as.numeric(q$open),
    high   = as.numeric(q$high),
    low    = as.numeric(q$low),
    close  = as.numeric(q$close),
    volume = as.numeric(q$volume)
  ) %>%
    filter(!is.na(date), !is.na(close))
}

flag_bad_history <- function(prices_tbl, min_nonzero_days = 20, min_total_days = 100) {
  prices_tbl %>%
    mutate(
      flat_bar = !is.na(open) & !is.na(high) & !is.na(low) & !is.na(close) &
        open == high & high == low & low == close,
      zero_vol = is.na(volume) | volume == 0
    ) %>%
    group_by(symbol) %>%
    summarise(
      n_days = n(),
      nonzero_vol_days = sum(!zero_vol, na.rm = TRUE),
      flat_zero_days = sum(flat_bar & zero_vol, na.rm = TRUE),
      all_flat_zero = all(flat_bar & zero_vol),
      .groups = "drop"
    ) %>%
    mutate(
      bad_history = all_flat_zero |
        nonzero_vol_days < min_nonzero_days |
        n_days < min_total_days
    )
}

run_open_to_close_strategy_curl <- function(stock_df,
                                            stop_loss = 0.03,
                                            lookback_days = 220,
                                            min_nonzero_days = 20,
                                            min_total_days = 100) {
  if (nrow(stock_df) == 0) {
    return(empty_results_tbl())
  }
  
  tickers <- stock_df %>%
    pull(Symbol) %>%
    unique()
  
  signal_dates <- unique(as.Date(stock_df$date))
  if (length(signal_dates) != 1) {
    stop("stock_df must contain exactly one signal date.")
  }
  
  signal_date <- signal_dates[1]
  from_date   <- signal_date - lookback_days
  to_date     <- signal_date + 10
  
  message("Running strategy for signal_date = ", signal_date)
  message("Universe size = ", length(tickers))
  
  prices_list <- purrr::map(tickers, ~ download_yahoo_chart(.x, from = from_date, to = to_date))
  names(prices_list) <- tickers
  
  failed <- names(prices_list)[purrr::map_lgl(prices_list, is.null)]
  if (length(failed) > 0) {
    message("Failed tickers: ", paste(failed, collapse = ", "))
  }
  
  valid_prices <- prices_list[!purrr::map_lgl(prices_list, is.null)]
  if (length(valid_prices) == 0) {
    message("No valid price histories downloaded.")
    return(empty_results_tbl())
  }
  
  prices <- bind_rows(valid_prices)
  
  quality_tbl <- flag_bad_history(
    prices,
    min_nonzero_days = min_nonzero_days,
    min_total_days   = min_total_days
  )
  
  bad_symbols <- quality_tbl %>%
    filter(bad_history) %>%
    pull(symbol)
  
  if (length(bad_symbols) > 0) {
    message("Excluded bad symbols: ", paste(bad_symbols, collapse = ", "))
  }
  
  prices <- prices %>%
    filter(!symbol %in% bad_symbols)
  
  if (nrow(prices) == 0) {
    message("All symbols were excluded by bad history filters.")
    return(empty_results_tbl())
  }
  
  signals <- prices %>%
    group_by(symbol) %>%
    arrange(date, .by_group = TRUE) %>%
    mutate(
      ma10  = zoo::rollmean(close, 10, fill = NA, align = "right"),
      ma20  = zoo::rollmean(close, 20, fill = NA, align = "right"),
      ma100 = zoo::rollmean(close, 100, fill = NA, align = "right")
    ) %>%
    ungroup()
  
  signal_tbl <- signals %>%
    filter(date == signal_date) %>%
    mutate(
      pass_filter = !is.na(ma10) & !is.na(ma20) & !is.na(ma100) &
        close > ma10 &
        ma10 > ma20 &
        close > ma100
    ) %>%
    select(symbol, signal_date = date, ma10, ma20, ma100, pass_filter)
  
  entry_tbl <- signals %>%
    filter(date > signal_date) %>%
    group_by(symbol) %>%
    slice_min(order_by = date, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    select(symbol, entry_date = date, open, high, low, close)
  
  trades <- signal_tbl %>%
    filter(pass_filter) %>%
    inner_join(entry_tbl, by = "symbol") %>%
    mutate(
      entry_price = open,
      stop_price  = entry_price * (1 - stop_loss),
      stop_hit    = low <= stop_price,
      exit_price  = if_else(stop_hit, stop_price, close),
      exit_reason = if_else(stop_hit, "stop_loss", "close_exit"),
      hold_days   = 1L,
      return_pct  = (exit_price / entry_price - 1) * 100
    )
  
  if (nrow(trades) == 0) {
    message("No trades passed the filter.")
    return(empty_results_tbl())
  }
  
  stock_df %>%
    rename(symbol = Symbol, name = Name) %>%
    select(
      -any_of(c(
        "ma10", "ma20", "ma100", "close_px",
        "close_above_ma10", "close_above_ma20",
        "ma10_above_ma20", "ma_setup_ok"
      ))
    ) %>%
    inner_join(trades, by = "symbol") %>%
    select(
      symbol, name,
      signal_date, entry_date,
      ma10, ma20, ma100,
      open, high, low, close,
      entry_price, stop_price, stop_hit,
      exit_price, exit_reason, hold_days, return_pct
    ) %>%
    distinct(symbol, signal_date, .keep_all = TRUE) %>%
    arrange(desc(return_pct))
}

upsert_results <- function(con, all_results) {
  if (nrow(all_results) == 0) {
    message("0 rows to upload. Exiting without DB write.")
    return(invisible(NULL))
  }
  
  results_to_upload <- all_results %>%
    mutate(
      symbol      = as.character(symbol),
      name        = as.character(name),
      signal_date = as.Date(signal_date),
      entry_date  = as.Date(entry_date),
      ma10        = as.numeric(ma10),
      ma20        = as.numeric(ma20),
      ma100       = as.numeric(ma100),
      open        = as.numeric(open),
      high        = as.numeric(high),
      low         = as.numeric(low),
      close       = as.numeric(close),
      entry_price = as.numeric(entry_price),
      stop_price  = as.numeric(stop_price),
      stop_hit    = as.logical(stop_hit),
      exit_price  = as.numeric(exit_price),
      exit_reason = as.character(exit_reason),
      hold_days   = as.integer(hold_days),
      return_pct  = as.numeric(return_pct)
    ) %>%
    distinct(symbol, signal_date, .keep_all = TRUE)
  
  DBI::dbWithTransaction(con, {
    DBI::dbWriteTable(
      con,
      "tmp_yahoo_52wk_trades",
      results_to_upload,
      temporary = TRUE,
      overwrite = TRUE
    )
    
    DBI::dbExecute(con, '
      INSERT INTO public.yahoo_52wk_trades (
        symbol, name, signal_date, entry_date,
        ma10, ma20, ma100,
        open, high, low, close,
        entry_price, stop_price, stop_hit,
        exit_price, exit_reason, hold_days, return_pct
      )
      SELECT
        symbol, name, signal_date, entry_date,
        ma10, ma20, ma100,
        open, high, low, close,
        entry_price, stop_price, stop_hit,
        exit_price, exit_reason, hold_days, return_pct
      FROM tmp_yahoo_52wk_trades
      ON CONFLICT (symbol, signal_date)
      DO UPDATE SET
        name        = EXCLUDED.name,
        entry_date  = EXCLUDED.entry_date,
        ma10        = EXCLUDED.ma10,
        ma20        = EXCLUDED.ma20,
        ma100       = EXCLUDED.ma100,
        open        = EXCLUDED.open,
        high        = EXCLUDED.high,
        low         = EXCLUDED.low,
        close       = EXCLUDED.close,
        entry_price = EXCLUDED.entry_price,
        stop_price  = EXCLUDED.stop_price,
        stop_hit    = EXCLUDED.stop_hit,
        exit_price  = EXCLUDED.exit_price,
        exit_reason = EXCLUDED.exit_reason,
        hold_days   = EXCLUDED.hold_days,
        return_pct  = EXCLUDED.return_pct;
    ')
  })
  
  check_n <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM public.yahoo_52wk_trades")
  message("yahoo_52wk_trades row count: ", check_n$n[[1]])
}

main <- function() {
  con <- db_connect_supabase()
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  
  DBI::dbGetQuery(con, "select 'connected' as status, now();") |>
    print()
  
  create_target_table(con)
  
  signal_date <- get_latest_usable_signal_date(con)
  if (is.na(signal_date)) {
    message("No usable signal date found in public.yahoo_52_week_gainers.")
    quit(save = "no", status = 0)
  }
  
  message("Using signal_date: ", signal_date)
  
  stock_df <- get_signal_universe(con, signal_date)
  if (nrow(stock_df) == 0) {
    message("No source rows found for signal_date: ", signal_date)
    quit(save = "no", status = 0)
  }
  
  message("Source rows pulled: ", nrow(stock_df))
  
  all_results <- tryCatch(
    run_open_to_close_strategy_curl(stock_df, stop_loss = 0.03),
    error = function(e) {
      message("Strategy failed for signal_date ", signal_date, ": ", e$message)
      empty_results_tbl()
    }
  )
  
  message("Trades produced: ", nrow(all_results))
  
  if (nrow(all_results) > 0) {
    print(all_results %>% count(entry_date))
  }
  
  upsert_results(con, all_results)
  
  message("Done.")
}

main()