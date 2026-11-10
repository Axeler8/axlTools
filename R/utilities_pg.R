# === tools for convinient Postgre data works around DBI ====
library("DBI")
library("RPostgreSQL") # - old lib for now.
# switch to RPostgres | RPostgres::Postgres() next update


# read rds with creds pre formatted ----
# legacy -- remove next update
readCreds <- function(credFileName = "sscdbcreds.rds", dir){
  authList <- readRDS(paste0(dir, "/", credFileName))
  return(authList)
}


# create AUTH obj to operate ----
# legacy -- rename next update
conPG <- function(dbcreds, ...) {
  con <- DBI::dbConnect(
    drv = RPostgreSQL::PostgreSQL(),
    host = dbcreds$host,
    dbname = dbcreds$dbname,
    user = dbcreds$user,
    password = dbcreds$password,
    port = dbcreds$port
    )
  return(con)
}


# chunked Postgre upload with volume\load parameters ----
pstUpload <- function(pstCon, data, tableName, disp_mode = c("append", "rewrite"),
                      timeout = 1L, max_size = 9L, max_load = 10L) {
  require("DBI")
  # подсчет объема одного куска таблицы для загрузки,
  # кол-во строк от размера объекта
  # с переметрами превышения (max_load) и размера одного куска (max_size)
  max_size <- max_size * 1000000 # Mbytes to bytes
  max_load <- 1 + (max_load / 100) # int percent to float num
  dt_rows <- nrow(data)
  size <- object.size(data) %>% as.integer()
  overload <- size / max_size %>% round()
  if(overload > max_load) {
    n_rows <- round(dt_rows / overload)
    seq_row <- seq.int(1, dt_rows, by = n_rows)
    if(seq_row[length(seq_row)] < dt_rows) {seq_row <- c(seq_row, dt_rows)}
  } else {seq_row <- c(1)}

  # remove table if rewrite needed
  if(disp_mode == "rewrite" & DBI::dbExistsTable(pstCon, tableName)){DBI::dbRemoveTable(pstCon, tableName)}
  # if(!DBI::dbExistsTable(pstCon, tableName)){DBI::dbCreateTable(pstCon, tableName, data[0])}

  # main load
  db_load <- lapply(1:length(seq_row), function(x){
    if(x >= length(seq_row) & x > 1) {
      message(paste(Sys.time(), seq_row[x], "total rows uploaded to", tableName))
      return(x)} else if (length(seq_row) == 1) {
        dt_short <- data} else {
          start <- seq_row[x]
          end <- (seq_row[x + 1]) - 1
          if(seq_row[x + 1] == seq_row[length(seq_row)]){end <- seq_row[x + 1]}
          dt_short <- data[start : end, ] }
    Sys.sleep(timeout)
    percent <- paste0(round(nrow(dt_short) / dt_rows * 100L, 1), "%")
    # print(head(dt_short, 10))
    DBI::dbWriteTable(pstCon, tableName, dt_short, append = TRUE, row.names = FALSE)
    message(paste(Sys.time(), nrow(dt_short), "rows uploaded,", percent), appendLF = TRUE)
    return(nrow(dt_short))
  })
}


# read\write dispatcher ----
# legacy -- remove next update
runPG2 <- function(credFile, dt = NULL, tableName,
                   mode = c("read", "rewrite", "append")) {
  # READ CREDS
  dbcreds <- readRDS(credFile)
  # connect
  con <- DBI::dbConnect(
    drv = RPostgreSQL::PostgreSQL(),
    host = dbcreds$host,
    dbname = dbcreds$dbname,
    user = dbcreds$user,
    password = dbcreds$password,
    port = dbcreds$port)
  # READ
  if(mode == "read") {
    userDT <- as.data.table(dbReadTable(con, tableName))
    dbDisconnect(con)
    return(userDT)
    # WRITE
  } else {
    userDT <- pstUpload(con, dt, tableName, disp_mode = mode)
    dbDisconnect(con)
    return(paste(tableName, "written to PG"))
  }
}


# read\write\delete\query dispatcher ----
# legacy -- remove next update
runPG3 <- function(credFile, tableName = NULL, dt = NULL,  query = NexULL,
                   mode = c("read", "rewrite", "append", "delete", "query")) {
  # auth
  dbcreds <- readRDS(credFile)
  con <- conPG(dbcreds)
  # method switch
  if (mode == "read") {
    if(!dbExistsTable(con, tableName)) {
      res <- paste0("ERROR: ", tableName, " not found for reading")
    } else {
      userDT <- as.data.table(dbReadTable(con, tableName))
      res <- userDT
    }

  } else if (mode == "delete") {
    if(!dbExistsTable(con, tableName)){
      res <- paste0("ERROR: ", tableName, "not found to delete")
    } else {
      dbRemoveTable(con, tableName)
      res <- paste0(tableName, ' deleted')
    }

  } else if (mode == "query") {
    if(is.null(query)) {
      res <- paste0("ERROR: SQL query required")
    } else {
      dt <- dbGetQuery(con, query)
      if (nrow(dt) < 1 ) {
        res <- paste0("0 rows for query: ", query)
      } else {
        res <- dt
      }}

  } else if (mode %in% c("rewrite", "append")) {
    userDT <- pstUpload(con, dt, tableName, disp_mode = mode)
    res <- paste0(tableName, " written to PG")

  } else {
    res <- paste0("ERROR: ", mode, " - WRONG MODE! Pick one: ", c("read", "rewrite", "append", "delete", "query"))
  }
  dbDisconnect(con)
  if (res %ilike% 'error'){stop(res)}
  paste("Successfuly did", mode, "on", fifelse(!is.null(tableName), tableName, query)) %>% sendLogs_mini()
  return(res)
}


# LATEST REFACTORED =====
# read & check auth file
readPGcred <- function(x) {
  if(!file.exists(x)){stop(paste(x, '- not found'))}
  PGcred <- readRDS(x)
  # check pg db attributes
  PGattrs <- c("host", "dbname", "user", 'password', 'port')
  PGattrsMiss <- PGattrs[!PGattrs %in% names(PGcred)]
  if(length(PGattrsMiss) >= 1){stop(paste(paste0(PGattrsMiss, collapse = ", "), "missing in", x))}
  # check attrs' values
  if(any(unlist(lapply(PGcred, is.na))) ||
     any(unlist(lapply(PGcred, is.null)))) {stop(paste(x, "- data missing"))}
  return(PGcred)
}

# AUTH obj
conPG2 <- function(x, tz = 'Europe/Moscow') {
  if(!is.list(x)){stop(paste(x, "must be named list"))}
  # ? TryCatch ?
  con <- DBI::dbConnect(
    drv = RPostgreSQL::PostgreSQL(),
    host = x[['host']],
    dbname = x[['dbname']],
    user = x[['user']],
    password = x[['password']],
    port = x[['port']]
  )
  DBI::dbExecute(con, paste0("SET TIME ZONE '", tz, "'"))
  return(con)
}


# core dispatcher ---- latest -----
runPG4 <- function(credFilePath, tableName = NULL, dt = NULL, query = NULL,
                   mode = c("read", "rewrite", "append", "delete", "query"),
                   verbose = TRUE, ...) {
  mode <- match.arg(mode)
  # ARGS CHECK
  if(mode == "query" && is.null(query)) { stop("SQL query required") }
  if(mode != "query" && is.null(tableName)) { stop(paste("tableName required to", mode)) }
  if(mode %in% c("rewrite", "append") && is.null(dt)) {stop(paste("data table\frame required as dt for", mode))}
  if(!is.null(dt) && !is.data.frame(dt)){stop(paste("dt arg must be of data table\frame class"))}
  # open & close db connection
  the_con <- conPG2(readPGcred(credFilePath))
  on.exit(dbDisconnect(the_con), add = TRUE)
  # RUN
  res <- switch(
    mode,
    read = {
      if(!dbExistsTable(the_con, tableName)) {stop(paste("ERROR! Table", tableName, "not found"))}
      as.data.table(dbReadTable(the_con, tableName))
    },
    delete = {
      if(!dbExistsTable(the_con, tableName)) {stop(paste("ERROR! Table", tableName, "not found"))}
      dbRemoveTable(the_con, tableName)
      paste0(tableName, " deleted")
    },
    query = {
      dt <- dbGetQuery(the_con, query)
      if (nrow(dt) == 0) {warning("0 rows returned")}
      dt
    },
    rewrite =, append = {
      pstUpload(the_con, dt, tableName, disp_mode = mode)
      paste0(tableName, " written to PG")
    })
  # Log
  if(verbose){paste("Successful", mode, "-", ifelse(!is.null(tableName), tableName, query)) |> sendMsg()}
  return(res)
}

# ALIAS
runPG <- runPG4

# Dynamic args list injection
makePGfun <- function(mode) {
  force(mode)
  function(...) {
    args <- list(...)
    args$mode <- mode
    do.call(runPG, args)
  }
}

# convinience shortcuts
readPG <- makePGfun("read")
deletePG <- makePGfun("delete")
appendPG <- makePGfun("append")
queryPG <- makePGfun("query")
rewritePG <- makePGfun("rewrite")
