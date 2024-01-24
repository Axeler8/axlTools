toTranslit_bq <- function(x = "чёщыфйшьъ:", verbose = FALSE) {
  require("stringi")
  y <- gsub('.*/ ?(\\w+)|\\(|\\)|^\\d+', '\\1', x)
  y <- gsub("[.]| |ʺ|-|\\s+", "_", y)
  y <- stringi::stri_trans_general(y, "russian-latin/bgn")
  y <- gsub("ʹ|ʺ|ʹʺ|ʺʺ", "", y)
  y <- gsub("\\%", "perc", y)
  if(verbose){writeLines(y)}
  return(y)
}


myFread <- function(fileUrl) {

  require("curl")
  require("data.table")

  if(!exists("hh")){

    message("curl MK authing")
    MK_creds <- readRDS("~/r/src/tokens/MK_creds")
    hh <<- curl::new_handle()
    userpwd <- MK_creds$cred %>% as.character()

    curl::handle_setopt(
      handle = hh,
      httpauth = 1,
      userpwd = userpwd)
  }

  tempDLdir <- "~/r/src/_temp_dl/"
  filename <- gsub(".*/", "", fileUrl)
  dest_file <- paste0(tempDLdir, filename)

  dl <- curl::curl_download(fileUrl,
                            destfile = dest_file,
                            mode = "wb", quiet = TRUE, handle = hh)

  dt <- fread(dl, colClasses = "character", encoding = "UTF-8")

  file.remove(dest_file)
  return(dt)

}

# appending fbig & vk leads
compareCols <- function (dt1, dt2) {

  exs1 <- names(dt1)[names(dt1) %in% names(dt2)]
  miss1 <- names(dt1)[!names(dt1) %in% names(dt2)]

  exs2 <- names(dt2)[names(dt2) %in% names(dt1)]
  miss2 <- names(dt2)[!names(dt2) %in% names(dt1)]

  res <- list("dt1_present_cols" = exs1,
              "dt2_present_cols" = exs2,
              "dt1_abscent_col" = miss1,
              "dt2_abscent_col" = miss2)

  return(res)
}

# paste with sys.time!
STpaste <- function(...){
  msg <- paste(Sys.time(), ...)
  return(msg)
}


BQdl <- function(SQLtext, project){
  if(class(SQLtext) != "character"){stop("input is always a character")}

  tryCatch({
    BQdt <- bigrquery::bq_project_query(x = project,
                                        query = SQLtext,
                                        use_legacy_sql = F) %>%
      bigrquery::bq_table_download() %>%
      as.data.table()
    return(BQdt)
  }, error = function(cond){
    message(cond)
  })
}

BQupload <- function(
    project = "etalon0919",
    dt, dataset, table,
    truncate = FALSE) {
  # подсчет оптимального объема таблицы для загрузки, пропорционально рабивая по строкам
  # размер объекта - не более 110% от константы BQ ~ 9MB
  max_size <- 9000000 # bytes
  max_load <- 1.1 # позднее можно вводить значения в аргументы функции

  # append or truncate
  if(truncate) {disposition <- "WRITE_TRUNCATE"} else {disposition <- "WRITE_APPEND"}

  # row count by dividing cur obj size by const max size
  # adjusting sequence for lapply'ing: last element - dropped, normal size dt is uploaded unsplit
  dt_rows <- nrow(dt)
  size <- object.size(dt)  %>% as.integer()
  overload <- size / max_size %>% round()
  if(overload > max_load) {
    n_rows <- round(dt_rows / overload)
    seq_row <- seq.int(1, dt_rows, by = n_rows)
    if(seq_row[length(seq_row)] < dt_rows) {seq_row <- c(seq_row, dt_rows)}
  } else {seq_row <- c(1)}

  BQload <- lapply(1:length(seq_row), function(x){
    if(x >= length(seq_row) & x > 1) {
      msg <- paste0(seq_row[x], "TOTAL rows uploaded to", table)
      message(STpaste(msg))
      return(msg)} else if(length(seq_row) == 1) {dt_short <- dt} else {
        start <- seq_row[x]
        end <- (seq_row[x + 1]) - 1
        if(seq_row[x + 1] == seq_row[length(seq_row)]){end <- seq_row[x + 1]}
        dt_short <- dt[start : end, ]
      }
    Sys.sleep(0.5)
    percent <- paste0(round(nrow(dt_short) / dt_rows * 100L), "%")

    tryCatch({
      bq_table(project = project,
               dataset = dataset,
               table   = table) %>%
        bq_table_upload(x = .,
                        values = dt_short,
                        create_disposition = "CREATE_IF_NEEDED",
                        write_disposition = disposition,
                        fields = dt)

      message(STpaste(nrow(dt_short), "rows uploaded,", percent), appendLF = TRUE)
      disposition <<- "WRITE_APPEND" #yeah, append anyway!
    }, error = function(cond){
      message(paste(Sys.time(), cond))
      return(table)
    })
  })
}


# BQupload с партиционированием по указанной колонке с датами\времени.
# только для новых таблиц!
BQupload_part <- function(
    project = "etalon0919",
    dt, dataset, table, max_size = 9, max_load = 10,
    truncate = FALSE, part_type = NULL, part_field = NULL) {
  # подсчет оптимального объема таблицы для загрузки, пропорционально рабивая по строкам
  # размер объекта - по умолчанию не более 10% от ~ 9MB по умолчанию.
  max_size <- max_size * 1000000 # Mbytes to bytes
  max_load <- 1 + (max_load / 100) # int percent to float num

  # append or truncate
  if(truncate) {disposition <- "WRITE_TRUNCATE"} else {disposition <- "WRITE_APPEND"}

  # нужна еще проверка на тип колонки и тип партиц-я
  if(!is.null(part_type) & !is.null(part_field)){

    tableCheck <- bigrquery::bq_table(project = project,
                                      dataset = dataset,
                                      table   = table) %>%
      bigrquery::bq_table_exists()
    if(tableCheck){
      cat(paste("\n", table, "already exists"))
    } else if(!tableCheck) {
      bigrquery::bq_table(project = project,
                          dataset = dataset,
                          table   = table) %>%
        bigrquery::bq_table_create(
          dt[1:5], fields = dt[1:5], timePartitioning = list(
            type = part_type,
            field = part_field))
      cat(paste(table," crataed and partioned by", part_field))
    }
  }

  # row count by dividing cur obj size by const max size
  # adjusting sequence for lapply'ing: last element - dropped, normal size dt is uploaded unsplit
  dt_rows <- nrow(dt)
  size <- object.size(dt)  %>% as.integer()
  overload <- size / max_size %>% round()
  if(overload > max_load) {
    n_rows <- round(dt_rows / overload)
    seq_row <- seq.int(1, dt_rows, by = n_rows)
    if(seq_row[length(seq_row)] < dt_rows) {seq_row <- c(seq_row, dt_rows)}
  } else {seq_row <- c(1)}

  BQload <- lapply(1:length(seq_row), function(x){
    if(x >= length(seq_row) & x > 1) {
      msg <- paste0(seq_row[x], "TOTAL rows uploaded to", table)
      message(STpaste(msg))
      return(msg)} else if(length(seq_row) == 1) {dt_short <- dt} else {
        start <- seq_row[x]
        end <- (seq_row[x + 1]) - 1
        if(seq_row[x + 1] == seq_row[length(seq_row)]){end <- seq_row[x + 1]}
        dt_short <- dt[start : end, ]
      }
    Sys.sleep(0.5)
    percent <- paste0(round(nrow(dt_short) / dt_rows * 100L), "%")

    tryCatch({
      bq_table(project = project,
               dataset = dataset,
               table   = table) %>%
        bq_table_upload(x = .,
                        values = dt_short,
                        create_disposition = "CREATE_IF_NEEDED",
                        write_disposition = disposition,
                        fields = dt)

      message(STpaste(nrow(dt_short), "rows uploaded,", percent), appendLF = TRUE)
      disposition <<- "WRITE_APPEND" #yeah, append anyway!
    }, error = function(cond){
      message(paste(Sys.time(), cond))
      return(table)
    })
  })
}



telega_send <- function (textt = "I SENT SMTHING THERE") {

  require("telegram.bot")
  tel_creds <- readRDS("~/r/tokens/tel_creds.rds")
  bot <- Bot(token = tel_creds$tok)

  # print(bot$getMe())
  updates <- bot$getUpdates()
  bot$sendMessage(chat_id = tel_creds$gr_chat_id, text = textt)
}



month2day <- function(mydate = as.Date(Sys.Date() - 1),
                      dt, dimention, backJoin = FALSE) {
  # на входе: 1) дата месяца по которому разбиваем 2) таблица с 2.1 строка-измерение, по которой бьем; 2.2 числовые колонки, которые разбиваются
  # на выходе: дата-тейбл с разбиением числовых параметров указанного месяца укаазнного измерения по дням. Так же можно прицепить к исходному

  # ----- ДОРАБОТКА:
  # - ВСЕ ОПЕРАЦИИ В DATA.TABLE!
  # - прдусмотреть вектор с перечислением dimention(чтобы до входа не делать сцепление строки в одну)
  # - предусмотреть работу с неск. месяцами
  # - колонка с днем в формате с данными

  # dt is data frame\table dimention is col name to split by

  month_start <- as.Date(cut(mydate, "month"))
  month_next <- max(seq(month_start, length = 2, by = "months"))
  month_end <- max(seq(month_start, length = 2, by = "months")) - 1

  days <- as.character(seq.Date(from = month_start, to = month_end, by = "day"))

  # src_medium <- unique(dt[[dimention]])
  dimentions <- unique(dt[[dimention]])

  days_dt <- data.frame(days = days,
                        id = "1")

  src_medium_dt <- data.frame(dimentions = dimentions,
                              id = "1")

  days_src_dt <- left_join(days_dt, src_medium_dt, by = "id")

  num_names <- dt %>%
    select_if(is.numeric) %>%
    names()


  days_src_dt_split <- days_src_dt %>%
    left_join(select(dt, all_of(c(num_names, dimention))), by = c("dimentions" = dimention))

  # count(d, src_medium, sort = T)
  days_src_dt_split[, num_names] <- days_src_dt_split[, num_names] / length(days)

  days_src_dt_split <- unique(days_src_dt_split)

  days_src_dt_split$id <- NULL
  days_src_dt_split$month_length <- length(days)

  names(days_src_dt_split)[names(days_src_dt_split) == "dimentions"] <- dimention
  names(days_src_dt_split)[names(days_src_dt_split) %in% num_names] <- paste0(names(days_src_dt_split)[names(days_src_dt_split) %in% num_names], "_day")

  days_src_dt_split <- days_src_dt_split %>%
    as.data.table()

  if(backJoin) {
    days_src_dt_split_joined <- days_src_dt_split %>%
      left_join(dt, by = dimention)

    return(days_src_dt_split_joined)

  } else {return(days_src_dt_split)}
}



subPhone <- function(phone, digits = 10L, ...){
  if(!is.integer(digits) | digits < 1L | digits > 10L) {stop("digits - interger, 1 - 10 ONLY")}

  symbols_pattern <- "\\+|\\s+|-|\\(|\\)"
  length_pattern <- paste0(".*(?=.{", digits, "}$)")

  cleanPhone <- gsub(symbols_pattern, "", phone)
  cleanPhone <- sub(length_pattern, "", cleanPhone, perl = TRUE)

  return(cleanPhone)
}


# fixing data.table's columns, by adding & removing
colsFix <- function(dt, main_cols, saveNew = TRUE, echo = TRUE){
  require("magrittr")
  if(!"data.table" %in% class(dt)){stop("works only for data.table objects!")}

  main_cols <- main_cols %>% unlist() %>% as.character()

  # unexpected cols
  extra_cols <- colnames(dt)[!colnames(dt) %in% main_cols]
  # missed cols
  miss_cols <- main_cols[!main_cols %in% colnames(dt)]

  dtName <- deparse(substitute(dt))

  if(length(extra_cols) > 0){
    if(saveNew){assign(paste0("new_", dtName), dt, envir = .GlobalEnv)}
    dt[, (extra_cols) := NULL]
    if(echo){cat(paste("\n", "removing extra cols:", extra_cols, "\n"))}
  } else {cat(paste("\n", "No extra cols found"))}

  if(length(miss_cols) > 0){
    dt[, (miss_cols) := NA_character_]
    if(echo){cat(paste("\n", "adding missings cols:", miss_cols, "\n"))}
  } else {cat(paste("\n", "All main cols found"))}
  return(dt)
}


# adjusting column types
colsTypefix <- function(dt, cols_list){

  require("data.table")
  # костыль, but that's it!

  if(!is.null(cols_list[["text_cols"]])){dt[, (cols_list[["text_cols"]]) := lapply(.SD, as.character), .SDcols = cols_list[["text_cols"]]]}
  if(!is.null(cols_list[["date_cols"]])){dt[, (cols_list[["date_cols"]]) := lapply(.SD, as.Date), .SDcols = cols_list[["date_cols"]]]}
  if(!is.null(cols_list[["num_cols"]])){dt[, (cols_list[["num_cols"]]) := lapply(.SD, as.numeric), .SDcols = cols_list[["num_cols"]]]}
  if(!is.null(cols_list[["logic_cols"]])){dt[, (cols_list[["logic_cols"]]) := lapply(.SD, as.logical), .SDcols = cols_list[["logic_cols"]]]}
  if(!is.null(cols_list[["int_cols"]])){dt[, (cols_list[["int_cols"]]) := lapply(.SD, as.integer), .SDcols = cols_list[["int_cols"]]]}
  if(!is.null(cols_list[["double_cols"]])){dt[, (cols_list[["double_cols"]]) := lapply(.SD, as.double), .SDcols = cols_list[["double_cols"]]]}
  return(dt)

}

# sending message prePasted with text label, through defferent outputs
sendLogs_short <- function(theMessage, vocal = FALSE,
                           tsPaste = TRUE, msgAlrt = TRUE,
                           tg = TRUE) {

  require("magrittr")
  if(length(theMessage) > 1){
    warning("the message should be of length 1, not", length(theMessage))
    theMessage <- theMessage[1] %>% as.character()
  }

  # e <- environment() # current environment
  # p <- parent.env(e)
  pf <- parent.frame()
  var <- pf$scriptName %>% as.character()
  if(length(var) < 1 || is.null(var) || is.na(var)){stop("scriptName not found!")}
  msg <- paste(var, theMessage)
  if(tsPaste == TRUE){msg <- paste(Sys.time(), msg, sep = "|")}

  # Консоль?
  if(msgAlrt == TRUE){message(msg)}

  # телега
  if(tg){telega_send(msg)}

  if(vocal){writeLines(msg)}
  return(msg)
}

# sending message prePasted with text label, through defferent outputs
sendLogs_mini <- function(theMessage, vocal = TRUE,
                          tsPaste = TRUE, msgAlrt = TRUE) {

  require("magrittr")
  if(length(theMessage) > 1){
    warning("the message should be of length 1, not", length(theMessage))
    theMessage <- theMessage[1] %>% as.character()
  }
  # msg <- paste(var, theMessage)

  if(tsPaste == TRUE){msg <- paste(Sys.time(), theMessage, sep = "|")}
  # Консоль
  if(msgAlrt == TRUE){message(msg)}
  if(vocal){writeLines(msg)}
  return(msg)
}

# turn nested cells into a text json
my2json <- function(x){
  require("magrittr")
  if(length(x) > 1 | class(x) == "list"){x <- x %>% jsonlite::toJSON() %>% as.character()}
  return(x)
  # vectorize!!!
}

# mass flattening nested lists
my2jsonlist <- function(list){
  require("data.table")
  list_flat <- lapply(1:length(list), function(x){
    item <- list[[x]]
    item_names <- names(item)
    item1 <- lapply(item, my2json)
    item2 <- as.data.table(rbind(item1))
    names(item2) <- item_names
    return(item2)
  })
  list_flat1 <- rbindlist(list_flat)
  return(list_flat1)
}

# mass flattening nested lists with fill
my2jsonlist2 <- function(list, toChar = TRUE){
  require("data.table")
  list_flat <- lapply(1:length(list), function(x){
    item <- list[[x]]
    item_names <- names(item)
    item1 <- lapply(item, my2json)
    item2 <- as.data.table(rbind(item1))
    names(item2) <- item_names
    return(item2)
  })
  list_flat1 <- rbindlist(list_flat, fill = TRUE)
  if(toChar){list_flat1 <- list_flat1[, colnames(list_flat1) := lapply(.SD, as.character), .SDcols = colnames(list_flat1)]}
  return(list_flat1)
}


intSpaceless <- function(data){
  data <- as.integer(iconv(data, "latin1", "ASCII", sub = ""))
  return(data)}

# calendar
clndr <- function(
    controlDay = format(Sys.Date() - 1L, "%A"),
    controlDayLag = 2L,
    start = as.Date("2022-09-01"),
    end = as.Date("2023-06-01"),
    ...){
  # extracting and filling report date (thursday-2) up the timeline
  # сделать категории точных четвертей
  # недели в календаре сочетать с границами чертвертей.
  require("data.table")
  weekDays <- weekdays(ISOdate(1, 1, 1:7))
  calendar <- data.table(date = seq.Date(start, end, "days"))
  # check for day name format
  if(!tolower(controlDay) %in% tolower(weekDays)){stop(paste("Control day must be like", weekDays, collapse = "|"))}

  realControlDay_n <- which(weekDays %in% controlDay) - controlDayLag
  realControlDay <- weekDays[realControlDay_n]
  calendar[, realControlDate := fifelse(format(date, "%A") == realControlDay, as.character(date), NA_character_)]
  calendar[, realControlDate := as.Date(realControlDate)]
  setorder(calendar, -date)
  calendar[, `:=`(realControlDate = realControlDate[nafill(replace(.I, is.na(realControlDate), NA), "locf")])]
  calendar$realControlDate[is.na(calendar$realControlDate)] <- max(calendar$realControlDate, na.rm = TRUE)
  calendar[, reportWeek := as.Date(realControlDate + controlDayLag)]

  return(calendar)
}


symbCount <- function(x, symbol){
  n <- lengths(regmatches(x, gregexpr(symbol, x)))
  return(n)}


markProject <- function(client = "Клиент", object = "Объект",
                        period = Sys.Date() - 1) {
  manualMPdata <- list(client = client, object = object, period_start = as.Date(cut(period, "month")),
                       period_end = max(seq(as.Date(cut(period, "month")), length = 2, by = "months") - 1))
  return(manualMPdata)
}

# generating table with dates for two last months to yesterday
makePeriod <- function(date = Sys.Date() - 1){
  require("data.table")
  curDate <- date
  monthStart <- as.Date(cut(curDate, "month"))
  lastMonthEnd <- monthStart - 1
  lastMonth <- as.Date(cut(lastMonthEnd, "month"))

  dt <- data.table(period_start = c(lastMonth, monthStart),
                   period_end = c(lastMonthEnd, curDate))
  return(dt)
}
