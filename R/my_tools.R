message("my tools loaded")

# transliteration around stringi package
toTranslit <- function(namess) {
  require("stringi")
  namess <- gsub("\\..*", "", namess)
  namess <- gsub('.*/ ?(\\w+)', '\\1', namess)
  namess <- gsub("[.]| |ʺ|-", "_", namess)
  namess <- gsub("-", ".", namess)
  # перевод в латиницу
  namess <- stri_trans_general(namess, "ru-ru_Latn/BGN")
  namess <- gsub("ʹ", "", namess)
  writeLines(namess)
  return(namess)
}

# same but to BigQuery naming standarts
toTranslit_bq <- function(x = "ёхйъёнич:ное", verbose = FALSE) {
  require("stringi")
  x <- gsub('.*/ ?(\\w+)|\\(|\\)|^\\d+', '\\1', x)
  x <- gsub("[.]| |ʺ|-|\\s+", "_", x)
  x <- stringi::stri_trans_general(x, "russian-latin/bgn")
  x <- gsub("ʹ|ʺ|ʹʺ|ʺʺ", "", x)
  x <- gsub("\\%", "perc", x)
  if(verbose){writeLines(x)}
  return(x)
}


# read_xlsx_allsheets <- function(filename) {
#   require("dplyr")
#   require("readxl")
#   require("purrr")
#   dt_fnl <- bind_rows(filename %>%
#                         excel_sheets() %>%
#                         set_names() %>%
#                         map(read_xlsx, path = filename, col_types = "text"))
#   return(dt_fnl)
# }

# generating table with months start and end
months_gen <- function(days_backward = 365L,
                       days_forward = 0L) {
  # здесь проверка - брать только положительные числа
  require("data.table")
  MyStart <- as.Date(cut(Sys.Date() - days_backward, "month"))
  MyEnd <- as.Date(cut(Sys.Date() + days_forward, "month"))
  date_start <- seq.Date(MyStart, MyEnd, by = "month")
  date_end <- date_start - 1
  date_start <- date_start[-length(date_start)]
  date_end <- date_end[-1]
  months <- data.table(date_start = date_start, date_end = date_end)
  return(months)
}

# wrapper around fread and curl_download with authentification
myFread <- function(fileUrl, creds = "~/r/src/tokens/MK_creds", dlDir = "~/r/src/_temp_dl/") {
  require("curl")
  require("data.table")
  if(!exists("hh")){
    writeLines("curl MK authing")
    MK_creds <- readRDS(creds)
    hh <<- curl::new_handle() # not the best solution but still
    userpwd <- MK_creds$cred %>% as.character()
    curl::handle_setopt(
      handle = hh,
      httpauth = 1,
      userpwd = userpwd)
  }
  filename <- gsub(".*/", "", fileUrl)
  dest_file <- paste0(dlDir, filename)
  curl::curl_download(fileUrl, destfile = dest_file,
                      mode = "wb", quiet = TRUE, handle = hh)
  dt <- fread(dl, colClasses = "character", encoding = "UTF-8")
  file.remove(dest_file)
  return(dt)
}

# comparing colnames of two tables
compareCols <- function (dt1, dt2) {
  res <- list("1st table's cols present in second" = names(dt1)[names(dt1) %in% names(dt2)],
              "1st table's missing cols" = names(dt1)[!names(dt1) %in% names(dt2)],
              "2nd table's cols present in first" = names(dt2)[names(dt2) %in% names(dt1)],
              "2nd table's missing cols" = names(dt2)[!names(dt2) %in% names(dt1)])
  return(res)
}

# paste with system time
STpaste <- function(...){
  msg <- paste(Sys.time(), ...)
  return(msg)
}


# wrapper around BigQuery table download
BQdl <- function(SQLtext){
  if(class(SQLtext) != "character" | length(SQLtext) > 1){stop("Only single length character")}
  tryCatch({
    BQdt <- bigrquery::bq_project_query(x = "etalon0919",
                                        query = SQLtext,
                                        use_legacy_sql = F) %>%
      bigrquery::bq_table_download() %>%
      as.data.table()
  return(BQdt)
  }, error = function(cond){
  message(cond)
 })
}

# wrapper around BigQuery upload with automatic split
BQupload <- function(project = "etalon0919", dt, dataset, table, truncate = FALSE) {
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


# Next version wrapper with options: table size in MBites, overload volume in percent and partition type and field
BQupload_part <- function(project = "etalon0919", dt, dataset, table,
                          max_size = 9, max_load = 10,
                          truncate = FALSE, part_type = NULL, part_field = NULL) {
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
      cat(paste("\n", table, "not partitioning existing table"))
          } else if(!tableCheck) {
            bigrquery::bq_table(project = project,
                          dataset = dataset,
                          table   = table) %>%
            bigrquery::bq_table_create(
              dt[1:1], fields = dt[1:1], timePartitioning = list(
                type = part_type, field = part_field))
        writeLines(paste(table, "created and partioned by", part_field))
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
    percent <- paste0(round(nrow(dt_short) / dt_rows * 100L), "%") # fix that counter!

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
      disposition <<- "WRITE_APPEND" # globalEnv is no good, will fix some day!
    }, error = function(cond){
      message(paste(Sys.time(), cond))
      return(table)
    })
  })
}

# telegram message sender
telega_send <- function (textt = "I SENT SMTHING THERE", tel_creds = "~/r/tokens/tel_creds.rds") {
  require("telegram.bot")
  tel_creds <- readRDS(tel_creds)
  bot <- Bot(token = tel_creds$tok)
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


# функция для рекурсивного обращения к АПИ ЦИАН офферов, через CURL (обертка HTTR) на случай временного отказа сервера
# можно задать количество попыток и паузу между ними
# на практике, после ошибки сервер отвечал со 2 раза, но по умолчанию - 5 попыток

cianGetResponse <- function(url = "URL link here", key,
                            attempts = 5L, pause = 1L, vocal = TRUE){
  require("httr")
  require("magrittr")

  # запускаем цикл из n попыток, при ответе ок или другой ошибке - ломаем цикл
  attempt <- 1L
  for (i in 1:attempts){
    Sys.sleep(pause)
    response <- tryCatch({
      res <- httr::GET(url, add_headers(authorization = key)) %>%
        content()
      if(vocal) {message(substr(STpaste(gsub(".*\\/", "", url), "OK"), 1, 75))}
      # сюда добавить обработку ответов сервера, на случай, если не 200 сам ответ
      assign("status", "OK", pos = parent.frame())
      return(res)
    }, error = function(cond){
      # message("wrong host")
      message(paste(Sys.time(), "error:",  cond))
      msg <- cond$message %>% as.character()

      # cat("\n", str(cond))
      if(grep("Could not resolve host", msg, ignore.case = TRUE)){
        status <<- "resolve"
      } else {stop("Stopping for other error")}
    })
    if(status == "OK") {
      return(response)
      break()
    } else if(response == "resolve") {
      if(i == attempts){stop(STpaste("stopping at attempt #", i, "of", attempts))}
      message(STpaste("Retrying request #", i, "of", attempts))
    } else {
      stop("Stopping for other error")}
  }
}


cianGetOffers <- function(
    # сбор данных ЦИАН OFFERS API.
  # на выходе list с data.table'ами
  # при ошибке сбора - ДТ с ошибкой

  key_path, clientName = "donstroy",
  dateFrom = Sys.Date() - 1,
  dateTo = Sys.Date() - 1,
  check = TRUE

  # token - в кабинете, заворачивается в rds
  # датs - вчерашний день
  # "2020-07-18"          - формат даты
  # "2020-07-18T00:00:00" - формат даты-времени
  # dateFrom <- as.Date("2022-01-01") - для сбора истории начиная с указанной даты
  # client_name вписывается в имя и таблицы, по нему идет связь
){
  require("httr")
  require("magrittr")
  require("data.table")
  require("bigrquery")
  require("xml2")

  key <- readRDS(key_path)

  # !!! TO DO BEFORE DEPLOY !!!  ----
  # запускать утром, после 5 минимум!
  # прописать типы данных у всех таблиц (сейчас много типов list)
  # rename cian_ to donstroy_!

  telega_send(STpaste("CIAN offers API started"))


  # 1. Основные офферы----
  cian_allOffers_DT <- tryCatch({
    limit <- 100
    url <- paste0("https://public-api.cian.ru/v2/get-my-offers?&page=1&pageSize=", limit)
    cian_responsе <- cianGetResponse(url, key = key)
    last_page <- ceiling(cian_responsе$result$totalCount / 100)
    cian_offers_dt <- cian_responsе$result$announcements %>% rbindlist()

    offrs <- lapply(2:last_page, function(x){

      url_next <- paste0("https://public-api.cian.ru/v2/get-my-offers?&page=",
                         x, "&pageSize=100")

      responsе <- cianGetResponse(url_next, key = key)
      dt <- responsе$result$announcements %>% rbindlist()
      Sys.sleep(0.5)
      return(dt)
    })

    offrs2 <- rbindlist(offrs) %>% rbind(cian_offers_dt) %>% unique()
    if(check){
      checker <- paste("CIAN Donstroy", length(offrs2$id),"offers,",
                       max(offrs2$creationDate, na.rm = TRUE), "latest")
      cat(STpaste(checker))
      telega_send(STpaste(checker))}

    offrs2[, dateParse := Sys.Date()]
    # return(offrs2)
  }, error = function(cond){
    msg <- cond$message %>% as.character()
    cat(STpaste(msg))
    telega_send(STpaste(msg))
    result <- data.table(dt = "cian_allOffers_DT", error = msg)
    return(result)
  })

  # 2. информация по ставкам на объявления----
  cian_auctionOffers_DT <- tryCatch({
    offer_limit <- 20L
    offer_step <- seq(1, length(cian_allOffers_DT$id), by = offer_limit)

    # проверить данные на правильность сбора и обработки!
    offer_list <- lapply(offer_step, function(x){
      # if(x == max(offers_step)){stop("end of line!")}
      start <- x
      finish <- x + 19
      if(finish > length(cian_allOffers_DT$id)){finish <- length(cian_allOffers_DT$id)}
      offers20 <- cian_allOffers_DT$id[start:finish]
      offerIds20 <- paste0("offerIds=", offers20) %>% paste0(collapse = "&")
      url <- paste0("https://public-api.cian.ru/v1/get-auction?", offerIds20)
      cian_responsе <- cianGetResponse(url, key = key)
      offer_auction <- cian_responsе$result$items %>% rbindlist(use.names = TRUE)
      Sys.sleep(0.5)
      return(offer_auction)
    })
    cian_auctionOffers_DT <- rbindlist(offer_list)
    cian_auctionOffers_DT[, dateParse := Sys.Date()]
    # return(cian_auctionOffers_DT)
  }, error = function(cond){
    msg <- cond$message %>% as.character()
    cat(STpaste(msg))
    telega_send(STpaste(msg))
    result <- data.table(dt = "cian_auctionOffers_DT", error = msg)
    return(result)
  })


  # 3 состояние импорта объявлений-----
  cian_latestImport_DT <- tryCatch({
    url <- paste0("https://public-api.cian.ru/v1/get-last-order-info")
    cian_responsе <- cianGetResponse(url, key = key)
    cian_latestImport_DT <- cian_responsе$result %>% unlist() %>% rbind() %>% as.data.table()

    if(check){
      checker <- paste("latest import date -", max(cian_latestImport_DT$lastFeedCheckDate, na.rm = TRUE))
      cat(checker)
      telega_send(STpaste(checker))}

    cian_latestImport_DT[, dateParse := Sys.Date()]
  }, error = function(cond){
    msg <- cond$message %>% as.character()
    cat(STpaste(msg))
    telega_send(STpaste(msg))
    result <- data.table(dt = "cian_latestImport_DT", error = msg)
    return(result)
  })

  # 4 отчет по импорту объявлений -----
  cian_offersImport_DT <- tryCatch({
    url <- "https://public-api.cian.ru/v1/get-order"
    cian_responsе <- cianGetResponse(url, key = key)

    cian_offersImport_DT <- cian_responsе$result$offers %>% rbindlist(fill = TRUE)
    cian_offersImport_DT[, `:=` (dateParse = Sys.Date(),
                                 errors = as.character(errors),
                                 warnings = as.character(warnings))]
  }, error = function(cond){
    msg <- cond$message %>% as.character()
    cat(STpaste(msg))
    telega_send(STpaste(msg))
    result <- data.table(dt = "cian_offersImport_DT", error = msg)
    return(result)
  })

  # # 5 отчет об изображениях ОТКЛЮЧЕН! ----
  # limit <- 500L
  # cian_imageList <- lapply(1:10, function(x){
  #   url <- paste0("https://public-api.cian.ru/v1/get-images-report?page=",x,
  #                 "&pageSize=", limit)
  # cian_responsе <- GET(url, add_headers(authorization = key)) %>%
  #     content()
  #   if(length(cian_responsе$result$items) != limit) {return(NA_character_)}
  #   dt <- cian_responsе$result$items %>% rbindlist(fill = TRUE)
  # return(dt)
  # })
  # cian_offerImages_DT <- cian_imageList %>% rbindlist(fill = TRUE) %>% unique()
  # # regroup by offerId
  # cian_offerImages_DT <- cian_offerImages_DT[, .(img_urls = paste0(unique(url), collapse = "|"),
  #                                                errorType = paste0(unique(errorType), collapse = "|"),
  #                                                errorText = paste0(unique(errorText), collapse = "|")),
  #                                            by = "offerId"]
  # cian_offerImages_DT[, dateParse := Sys.Date()]


  # 6 охват по объявлению----
  # по каждому объявлению
  # разбить по дням
  cian_searchCov_DT <- tryCatch({
    offer_ids <- cian_allOffers_DT$id %>% unique()

    offer_stat_list <- lapply(offer_ids, function(x){
      day_seq <- seq.Date(dateFrom, dateTo, by = "days")
      day_offer <- lapply(day_seq, function(xx) {

        url <- paste0("https://public-api.cian.ru/v1/get-search-coverage?offerId=", x,
                      "&dateFrom=", xx, "&dateTo=", xx)

        cian_responsе <- cianGetResponse(url, key = key)
        dt <- cian_responsе$result %>% rbind() %>% as.data.table()
        dt[, date := xx]
        # cat(paste0("id - ", x, ". date - ", xx, "\n"))
        Sys.sleep(0.4)
        return(dt)
      })

      day_offer_dt <- rbindlist(day_offer, use.names = TRUE)
      # Sys.sleep(1)
      return(day_offer_dt)
    })

    cian_searchCov_DT <- rbindlist(offer_stat_list, use.names = TRUE, fill = TRUE)
    int_cols <- colnames(cian_searchCov_DT)[!colnames(cian_searchCov_DT) %in% "date"]
    cian_searchCov_DT[, (int_cols) := lapply(.SD, as.integer), .SDcols = int_cols]
    # setcolorder(cian_searchCov_DT, c("date", "offerId"))
  }, error = function(cond){
    msg <- cond$message %>% as.character()
    cat(STpaste(msg))
    telega_send(STpaste(msg))
    result <- data.table(dt = "cian_searchCov_DT", error = msg)
    return(result)
  })

  # 7 статистика просмотров и показов телефонов по дням ----
  cian_offerDayViews_DT <- tryCatch({
    offer_ids <- cian_allOffers_DT$id %>% unique()
    offer_dayViews_list <- lapply(offer_ids, function(x){

      url <- paste0("https://public-api.cian.ru/v1/get-views-statistics-by-days?offerId=", x,
                    "&dateFrom=", dateTo, "&dateTo=", dateTo)

      cian_responsе <- cianGetResponse(url, key = key)
      phoneDayViews <- cian_responsе$result$phoneShowsByDays %>% rbindlist()
      dayViews <- cian_responsе$result$viewsByDays %>% rbindlist()

      # # а если по одному полю 0, а по другому есть значения?
      if(nrow(phoneDayViews) < 1 | nrow(dayViews) < 1){
        dt <- data.table()
        return(dt)}

      allViews <- dayViews[phoneDayViews, on = "date"]
      allViews$offerId <- cian_responsе$result$offerId
      Sys.sleep(0.4)
      return(allViews)
    })
    cian_offerDayViews_DT <- rbindlist(offer_dayViews_list, use.names = TRUE)
  }, error = function(cond){
    msg <- cond$message %>% as.character()
    cat(STpaste(msg))
    telega_send(STpaste(msg))
    result <- data.table(dt = "cian_offerDayViews_DT", error = msg)
    return(result)
  })

  # # 8 статистика просмотров и показов телефонов за все время !!! ОТКЛЮЧЕН!!! ----
  # offer_limit <- 50L
  # offer_step <- seq(1, length(cian_allOffers_DT$id), by = offer_limit)
  # allOffers_list <- lapply(offer_step, function(x){
  #   start <- x
  #   finish <- x + 49
  #   offers50 <- cian_allOffers_DT$id[start:finish]
  #   offerIds <- paste0("offersIds=", offers50) %>% paste0(collapse = "&")
  #   url <- paste0("https://public-api.cian.ru/v1/get-views-statistics?", offerIds)
  #   cian_responsе <- GET(url, add_headers(authorization = key)) %>%
  #     content()
  #   offer_allViews_DT <- cian_responsе$result$statistics %>%  rbindlist()
  #   return(offer_allViews_DT)
  # })
  # cian_phoneViewsOffers_DT <- rbindlist(allOffers_list, use.names = TRUE)
  # cian_phoneViewsOffers_DT[, dateParse := Sys.Date()]
  # # setcolorder(cian_phoneViewsOffers_DT, c("dateParse", "offerId"))


  # # 9 отчет по звонкам !!! ОТКЛЮЧЕН!!! ----
  # limit <- 100
  # url <- paste0("https://public-api.cian.ru/v2/get-calls-report?page=1&pageSize=", limit,
  #               "&dateFrom=", dateTo - 40,
  #               "&dateTo=", dateTo)
  # cian_responsе <- GET(url, add_headers(authorization = key)) %>%
  #   content()
  # cian_calls <- cian_responsе$result$calls
  # # last_page <- ceiling(cian_responsе$result$totalCount / 100)


  # 10 текущий баланс----
  cian_balance_DT <- tryCatch({
    url <- "https://public-api.cian.ru/v1/get-my-balance"
    cian_responsе <- cianGetResponse(url, key = key)
    cian_balance_DT <- cian_responsе$result %>% rbind() %>% as.data.table()
    # all to character
    cian_balance_DT <- cian_balance_DT[, lapply(.SD, as.character), .SDcols = colnames(cian_balance_DT)]
    cian_balance_DT[, dateParse := Sys.Date()]
    cian_balance_DT[, heldAmount := NULL]
  }, error = function(cond){
    msg <- cond$message %>% as.character()
    cat(STpaste(msg))
    telega_send(STpaste(msg))
    result <- data.table(dt = "cian_balance_DT", error = msg)
    return(result)
  })


  # 11 Активные услуги по объявлениям----
  cian_allServices_DT <- tryCatch({
    offer_limit <- 49L
    offer_step <- seq(1, length(cian_allOffers_DT$id), by = offer_limit)

    allServices_list <- lapply(offer_step, function(x){

      start <- x
      finish <- x + 48
      offers50 <- cian_allOffers_DT$id[start:finish]
      offerIds <- paste0("offerIds=", cian_allOffers_DT$id[1:offer_limit]) %>% paste0(collapse = "&")
      url <- paste0("https://public-api.cian.ru/v1/get-offer-active-services?", offerIds)
      cian_responsе <- cianGetResponse(url, key = key)
      services_offers <- data.table()
      services_offers_list <- lapply(1:length(cian_responsе$result$items), function(x){
        dt <- cian_responsе$result$items[[x]] %>% unlist() %>% rbind() %>% as.data.table()
        return(dt)
      })
      services_offers_DT <- rbindlist(services_offers_list)
      services_offers_DT <- rbind(services_offers, services_offers_DT)
      # cat(start, finish, "\n")
      Sys.sleep(1)
      return(services_offers_DT)

    })
    cian_allServices_DT <- rbindlist(allServices_list)
  }, error = function(cond){
    msg <- cond$message %>% as.character()
    cat(STpaste(msg))
    telega_send(STpaste(msg))
    result <- data.table(dt = "cian_allServices_DT", error = msg)
    return(result)
  })

  # 12 список операций----
  cian_operations_DT <- tryCatch({
    size <- 100L
    page <- 1
    url <- paste0("https://public-api.cian.ru/v1/get-operations?", "page=", page, "&pageSize=", size,
                  "&from=", dateFrom - 1, "T00:00:00","&to=", dateTo, "T00:00:00", "&operationType=all")

    cian_responsе <- cianGetResponse(url, key = key)
    cian_operations_DT <- cian_responsе$result$operations %>% rbindlist(use.names = TRUE, fill = TRUE)
  }, error = function(cond){
    msg <- cond$message %>% as.character()
    cat(STpaste(msg))
    telega_send(STpaste(msg))
    result <- data.table(dt = "cian_operations_DT", error = msg)
    return(result)
  })

  # 3.1 parsing xml feed to flat Data Table ----
  cian_FEEDoffers_DT <- tryCatch({
    message("reading XML FEED")
    feedUrl <- cian_latestImport_DT$activeFeedUrls
    cian_feed <- xml2::read_xml(feedUrl)
    cian_length <- xml2::xml_length(cian_feed)

    # 1st element is obtained earlier
    objects_seq <- 2:cian_length
    objss <- lapply(objects_seq, function(xx){
      obj <- xml_child(cian_feed, xx)
      onj_n <- xml_length(obj)
      # recursing main object entity
      nodess <- lapply(1:onj_n, function(x) {

        node <- xml_child(xml_child(cian_feed, xx), x)
        node_length <- xml_length(node)
        node_name <- xml_name(node)
        node_text <- xml_text(node)
        # сделать проверку на вложенность
        # if(node_length > 0){node_text <- xml_text(node) %>% jsonlite::toJSON() }
        length_name_text <- list("node_name" = node_name,
                                 # "node_length" = node_length,
                                 "node_text" = node_text)
        return(length_name_text)
      })
      nodessDT <- rbindlist(nodess, use.names = TRUE) %>% transpose(make.names = "node_name")
      return(nodessDT)
    })

    cian_FEEDoffers_DT <- rbindlist(objss, fill = TRUE)
    cian_FEEDoffers_DT[, dateParse := Sys.Date()]
  }, error = function(cond){
    msg <- cond$message %>% as.character()
    cat(STpaste(msg))
    telega_send(STpaste(msg))
    result <- data.table(dt = "cian_FEEDoffers_DT", error = msg)
    return(result)
  })

  # final tables ----
  # picking all tables names for mass upload
  cian_dts <- ls(pattern = "^cian_.*_DT$")
  # # cian_dts <- cian_dts[cian_dts!="cian_imageReport_DT"]
  # cian_dts_rewrite <- c("cian_offerImages_DT", "cian_allServices_DT")
  # cian_dts_append <- cian_dts[!cian_dts %in% cian_dts_rewrite]

  # # check for empty
  # lapply(cian_dts_append, function(x){
  #   dt <- get(x)
  #   if(nrow(dt) < 1) {
  #     telega_send(STpaste(x, "- EMPTY TABLE"))
  #     cian_dts_append <<- cian_dts_append[!cian_dts_append %in% x]
  #   }
  # })

  cian_dts_append_list <- lapply(cian_dts, function(x){
    dt <- get(x, as.environment(-1))
    return(dt)
  })

  names(cian_dts_append_list) <- paste0(clientName, "_", cian_dts)
  return(cian_dts_append_list)
}


# сбор данных ЦИАН АПИ заястройщики (данны по объектам)
# на выходе список с дата тейблами
cianGetObjects <- function(keyPath, clientName,
                           dateFrom = Sys.Date() - 1,
                           dateTo = Sys.Date() - 1,
                           check = TRUE

                           # token - в кабинете
                           # dateFrom <- as.Date("2022-01-01") для сбора истории
                           # "2020-07-18" - формат даты  на входе
                           # "2020-07-18T00:00:00" формат даты-времени
){
  require("httr")
  require("jsonlite")
  require("magrittr")
  require("data.table")

  telega_send(STpaste("CIAN builders API started"))
  key <- readRDS(keyPath)

  # ЗАПРОСЫ ----
  # 1 статистика по ЖК за вчера----
  url <- paste0("https://public-api.cian.ru/v1/get-newbuilding-statistics/?date=", dateTo)
  cian_responsе <- cianGetResponse(url, key = key)
  # если не возвращает объекты за вчера, пропускаем следующий этап
  if(length(cian_responsе$result$items) < 1L) {
    cat(paste("\n", "No new buildings on", dateTo))
  } else {
    cian_buildings_DT <- cian_responsе$result$items %>% rbindlist()
    cian_buildings_DT[, dateParse := dateTo]

    # 2 инфа общая по ЖК----

    newBuildings_list <- lapply(cian_buildings_DT$id, function(x){
      url <- paste0("https://public-api.cian.ru/v1/get-newbuilding/?newbuildingId=", x)
      cian_responsе <- cianGetResponse(url, key = key)
      dt <- cian_responsе$result$newbuilding %>% rbind() %>% as.data.table()
      return(dt)
    })

    cian_newBuildings_DT <- rbindlist(newBuildings_list, use.names = TRUE)
    cian_newBuildings_DT[, (colnames(cian_newBuildings_DT)) := lapply(.SD, as.character), .SDcols = colnames(cian_newBuildings_DT)]
    cian_newBuildings_DT[, dateParse := as.Date(dateTo)]

    if(check){
      builds <- paste0(unique(cian_newBuildings_DT$name), collapse = "|")
      checker <- paste("CIAN Donstroy", nrow(cian_buildings_DT), "buidlings parsed:", builds)
      cat(STpaste(checker))
      telega_send(STpaste(checker))
    }
  }

  # 3 buldings offers daily stats----
  # собрается по одному дню
  cian_offersStats_DT <- tryCatch({
    url <- paste0("https://public-api.cian.ru/v1/get-newbuilding-offer-statistics/?date=", dateFrom)
    cian_responsе <- cianGetResponse(url, key = key)
    cian_offersStats_DT <- cian_responsе$result$items %>% rbindlist()

    if(nrow(cian_offersStats_DT) > 0){
      cian_offersStats_DT$dateParse <- dateFrom

      if(check){
        checker <- paste("Offers stats:", max(cian_offersStats_DT$dateParse, na.rm = TRUE),
                         sum(cian_offersStats_DT$uniqueVisitors, na.rm = TRUE), "visitors")
        cat(STpaste(checker))
        telega_send(STpaste(checker))
      }
    }
    cian_offersStats_DT <- cian_offersStats_DT %>% as.data.table()
  }, error = function(cond){
    msg <- cond$message %>% as.character()
    cat(STpaste(msg))
    telega_send(STpaste(msg))
    result <- data.table(dt = "cian_offersStats_DT", error = msg)
    return(result)
  })

  # # 4 статистика просмотров и показов телефонов за все время----
  # пропускаем сейчас
  # ids_limit <- 50
  # offerIds <- unique(cian_offersStats_DT$id)
  # offer_step <- seq(1, length(offerIds), by = ids_limit)
  #
  # offerAllStats_list <- lapply(offer_step, function(x){
  #
  #   start <- x
  #   finish <- x + 49
  #
  #   offerIds_50 <- jsonlite::toJSON(list("offersIds" = offerIds[start:finish]))
  #
  #   url <- paste0("https://public-api.cian.ru/v1/get-newbuilding-offer-views-statistics/")
  #
  #   cian_responsе <- POST(url, body = offerIds_50, add_headers(authorization = key)) %>%
  #     content()
  #
  #   dt <- cian_responsе$result$statistics %>% rbindlist()
  #
  #   return(dt)
  # })
  #
  # cian_offerAllStats_DT <- rbindlist(offerAllStats_list, use.names = TRUE)
  # cian_offerAllStats_DT[, dateParse := dateTo]


  # 5 Аукцион застройщиков----
  cian_AUCT_DT <- tryCatch({
    url <- paste0("https://public-api.cian.ru/v1/get-builder-auctions/")
    cian_responsе <- cianGetResponse(url, key = key)
    AUCT_list <- cian_responsе$result$newbuildings

    # adjust rooms & auction differentely, then join it horiz-ly
    # auction table
    AUCT_list_full <- lapply(1:length(AUCT_list), function(x){
      AUCT <- AUCT_list[[x]]
      AUCT[["roomTypeSearchAnnouncementsPositions"]] <- NULL
      dt <- unlist(AUCT) %>% rbind() %>% as.data.table()
      return(dt)
    })

    cian_AUCT_DT <- rbindlist(AUCT_list_full, fill = TRUE)
    colnames(cian_AUCT_DT) <- gsub("\\.", "_", colnames(cian_AUCT_DT))

    # rooms table
    rooms_list <- lapply(1:length(AUCT_list), function(x){
      rooms <- AUCT_list[[x]][["roomTypeSearchAnnouncementsPositions"]]

      if(length(rooms) > 0) {
        rooms <- rooms %>%
          data.table::rbindlist(use.names = T) %>%
          data.table::transpose(make.names = "roomsCountTypeName")

        rooms$newbuildingId<- AUCT_list[[x]][["newbuildingId"]]
        return(rooms)

      } else {
        r <- data.table()
        return(r)
      }
    })

    cian_rooms_DT <- rbindlist(rooms_list, fill = TRUE)

    # не всегда отдаются все типы комнат,
    # отсутствующие - заполняются для дописывания в базу
    # упорядочиваются и переименовываются в англ.
    room_cols <- c("1–комн.", "2–комн.", "3–комн.", "4–комн. и более",
                   "Студии", "Своб. планировка", "newbuildingId")
    room_cols_eng <- c("room1", "room2", "room3", "room4", "roomst",
                       "roomfree", "newbuildingId")
    room_cols_mis <- room_cols[!room_cols %in% colnames(cian_rooms_DT)]
    if(length(room_cols_mis) > 0){
      cian_rooms_DT[, (room_cols_mis) := NA_integer_]
    }
    # translate to eng
    setcolorder(cian_rooms_DT, room_cols)
    cian_rooms_DT <- setNames(cian_rooms_DT, room_cols_eng)
    # joining tables - auction & rooms
    cian_AUCT_DT[, newbuildingId := as.integer(newbuildingId)]
    cian_AUCT_DT <- cian_rooms_DT[cian_AUCT_DT, on = "newbuildingId"]
    cian_AUCT_DT[, date := Sys.Date()]
    cian_AUCT_DT[, isAuctionEnabled := as.integer(isAuctionEnabled)]
  }, error = function(cond){
    msg <- cond$message %>% as.character()
    cat(STpaste(msg))
    telega_send(STpaste(msg))
    result <- data.table(dt = "cian_AUCT_DT", error = msg)
    return(result)
  })
  # setcolorder(cian_AUCT_DT, c("date", "newbuildingId", "currentBet", "stepBet", "liquidityRatio"))

  # 6 позиции и ставку в каталогах----
  # здесь можно попробовать по разным параметрам ?buildersPosition , ?newbuildingsPosition
  cian_auctBetsPos_DT <- tryCatch({
    auctBetsPos_list <- lapply(1:nrow(cian_AUCT_DT), function(x){
      bId <- cian_AUCT_DT$newbuildingId[x]
      bet <- cian_AUCT_DT$currentBet[x]
      if(is.na(bet)) {bet <- 500}

      url <- paste0("https://public-api.cian.ru/v1/get-calculation-builder-auction-bet/?newbuildingId=",
                    bId, "&bet=", bet)
      cian_responsе <- cianGetResponse(url, key = key)
      auctBetsPos <- cian_responsе$result %>% rbind() %>% as.data.table()
      auctBetsPos$newbuildingId <- bId
      return(auctBetsPos)
    })
    cian_auctBetsPos_DT <- rbindlist(auctBetsPos_list, fill = TRUE)
    cian_auctBetsPos_DT[, (colnames(cian_auctBetsPos_DT)) := lapply(.SD, as.character), .SDcols = colnames(cian_auctBetsPos_DT)]
    cian_auctBetsPos_DT[, date := Sys.Date()]
    int_cols <- colnames(cian_auctBetsPos_DT)[!colnames(cian_auctBetsPos_DT) %in% "date"]
    cian_auctBetsPos_DT[, (int_cols) := lapply(.SD, as.integer), .SDcols = int_cols]
  }, error = function(cond){
    msg <- cond$message %>% as.character()
    cat(STpaste(msg))
    telega_send(STpaste(msg))
    result <- data.table(dt = "cian_auctBetsPos_DT", error = msg)
    return(result)
  })


  # 7 звонки пользователя----
  # one date
  cian_userCalls_DT <- tryCatch({
    url <- paste0("https://public-api.cian.ru/v1/get-builder-calls/?onDate=", dateFrom, "T00:00:00")
    cian_responsе <- cianGetResponse(url, key = key)
    cian_userCalls_DT <- cian_responsе$result$calls %>% rbindlist(fill = TRUE)
    if(nrow(cian_userCalls_DT) > 0) {
      cian_userCalls_DT[, colnames(cian_userCalls_DT):= lapply(.SD, as.character), .SDcols=colnames(cian_userCalls_DT)]
      int_cols <- c("callDuration", "tariffPrice", "auctionPrice", "totalPrice")
      log_cols <- c("isCallback", "hasClaim")
      cian_userCalls_DT[, (int_cols) := lapply(.SD, as.integer), .SDcols = int_cols]
      cian_userCalls_DT[, (log_cols) := lapply(.SD, as.logical), .SDcols = log_cols]
      cian_userCalls_DT[, callDateTime := as.POSIXct(callDateTime, format="%Y-%m-%dT%H:%M:%S")]
    }
  }, error = function(cond){
    msg <- cond$message %>% as.character()
    cat(STpaste(msg))
    telega_send(STpaste(msg))
    result <- data.table(dt = "cian_userCalls_DT", error = msg)
    return(result)
  })

  # final tables ----
  cian_dts <- ls(pattern = "^cian_.*_DT$")
  # cian_dts <- cian_dts[cian_dts != "cian_rooms_DT"]

  # # check for blank tables (errors or no data)
  # empty <- lapply(cian_dts, function(x){
  #   dt <- get(x)
  #   if(nrow(dt) < 1 | ncol(dt) < 2) {
  #     telega_send(STpaste("CIAN EMPTY TABLE -", x))
  #     return(x)
  #   }
  #   return(NULL)
  # })
  #
  # empty1 <- empty[lengths(empty) >= 1] %>% unlist() %>% as.character()
  #
  # if(length(empty)>=1){
  #   cian_dts <- cian_dts[!cian_dts %in% empty]
  # }
  # cat("\n", cian_dts)

  cian_dts_list <- lapply(cian_dts, function(x){
    dt <- get(x, as.environment(-1))
    return(dt)
  })

  names(cian_dts_list) <- paste0(clientName, "_", cian_dts)
  return(cian_dts_list)
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


markProject <- function(client = "Вэб.рф", object = "Микрогород в лесу",
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
