# ----
# These functions were created & modified with the assistance of mighty DeepSeek AI
# (https://www.deepseek.com)
# ----


#' Chunked BigQuery Upload with Partitioning
#'
#' @param project,dataset,table BigQuery destination
#' @param dt data.table/data.frame to upload
#' @param max_size Max chunk size in MB (default 9MB)
#' @param max_load Force chunking if object size exceeds this % of max_size (default 10%)
#' @param truncate Overwrite table? (FALSE = append)
#' @param part_type Partition type ("DAY"/"HOUR"/etc.)
#' @param part_field Partition column name
#' @param quiet Suppress progress messages?
#' @return Invisible list of upload results
BQupload_part <- function(project, dt, dataset, table,
                          max_size = 9, max_load = 10,
                          truncate = FALSE, part_type = NULL,
                          part_field = NULL, quiet = FALSE) {

  # --- Input Validation ---
  stopifnot(
    requireNamespace("bigrquery", quietly = TRUE),
    requireNamespace("data.table", quietly = TRUE),
    is.data.frame(dt) || data.table::is.data.table(dt),
    nrow(dt) > 0,
    is.character(part_field) || is.null(part_field)
  )

  # --- Initial Setup ---
  max_bytes <- max_size * 1e6  # Convert MB to bytes
  load_threshold <- 1 + (max_load / 100)
  disposition <- if (truncate) "WRITE_TRUNCATE" else "WRITE_APPEND"
  bq_ref <- bigrquery::bq_table(project, dataset, table)

  # --- Partition Handling ---
  if (!is.null(part_type) && !is.null(part_field)) {
    if (bigrquery::bq_table_exists(bq_ref)) {
      if (!quiet) message("\n", table, " exists - using existing partition schema")
    } else {
      bigrquery::bq_table_create(
        bq_ref,
        fields = dt[1, ],  # Schema from first row
        timePartitioning = list(
          type = part_type,
          field = part_field,
          expirationMs = NULL  # Optional: add partition expiration
        )
      )
      if (!quiet) message(table, " created with ", part_type, " partitioning on ", part_field)
    }
  }

  # --- Chunking Logic ---
  dt_rows <- nrow(dt)
  total_size <- as.integer(object.size(dt))
  should_chunk <- (total_size / max_bytes) > load_threshold

  seq_row <- if (should_chunk) {
    n_chunks <- ceiling(total_size / max_bytes)
    chunk_size <- ceiling(dt_rows / n_chunks)
    unique(c(seq(1, dt_rows, chunk_size), dt_rows + 1))  # +1 for inclusive end
  } else {
    c(1, dt_rows + 1)  # Single chunk
  }

  # --- Upload Process ---
  upload_results <- lapply(seq_len(length(seq_row) - 1), function(i) {
    chunk <- dt[seq_row[i]:(seq_row[i+1] - 1), ]
    pct <- paste0(round(nrow(chunk)/dt_rows * 100), "%")

    if (!quiet) message(
      sprintf("[%s] Uploading %s rows (%s)",
              format(Sys.time(), "%H:%M:%S"),
              nrow(chunk), pct)
    )

    tryCatch({
      bigrquery::bq_table_upload(
        bq_ref,
        values = chunk,
        create_disposition = "CREATE_IF_NEEDED",
        write_disposition = if (i == 1) disposition else "WRITE_APPEND",
        fields = NULL  # Schema inferred if new table
      )
      list(success = TRUE, rows = nrow(chunk))
    }, error = function(e) {
      list(success = FALSE, error = conditionMessage(e))
    })
  })

  # --- Result Summary ---
  if (!quiet) {
    success_rate <- mean(sapply(upload_results, function(x) x$success))
    message(sprintf(
      "\nUpload complete: %s/%s chunks succeeded (%.1f%%)",
      sum(sapply(upload_results, function(x) x$success)),
      length(upload_results),
      success_rate * 100
    ))
  }

  invisible(upload_results)
}



#' Quickly convert column types in a data.table ----
#'
#' @param dt A data.table
#' @param cols_list Named list of vectors (e.g., list(text_cols = c("col1","col2")))
#' @return The modified data.table (by reference)
#' @examples
#' dt <- data.table(a = 1:3, b = c("1","2","3"))
#' colsTypefix(dt, list(num_cols = "a", text_cols = "b"))
colsTypefix <- function(dt, cols_list) {
  stopifnot(
    data.table::is.data.table(dt),
    is.list(cols_list),
    !is.null(names(cols_list)))

    # Conversion handlers
    type_handlers <- list(
      text_cols = as.character,
      date_cols = as.Date,
      num_cols = as.numeric,
      logic_cols = as.logical,
      int_cols = as.integer,
      double_cols = as.double
    )

    # Validate column existence first
    missing_cols <- setdiff(unlist(cols_list), names(dt))
    if (length(missing_cols)) {
      warning("Missing columns: ", paste(missing_cols, collapse = ", "))
      cols_list <- lapply(cols_list, function(x) intersect(x, names(dt)))
    }

    # Apply conversions
    for (type in names(type_handlers)) {
      if (!is.null(cols_list[[type]])) {
        dt[, (cols_list[[type]]) := lapply(.SD, type_handlers[[type]]),
           .SDcols = cols_list[[type]]]
      }
    }

    invisible(dt)  # Return silently (modifies by reference anyway)
}



#' Quickly convert column types in a data.table WITH Factor Support ----

colsTypefix <- function(dt, cols_list, ordered_factors = FALSE,
                        factor_levels = NULL, drop_unused_levels = TRUE) {
  stopifnot(
    data.table::is.data.table(dt),
    is.list(cols_list),
    !is.null(names(cols_list)))

    # Enhanced type handlers
    type_handlers <- list(
      text_cols = as.character,
      date_cols = as.Date,
      num_cols = as.numeric,
      logic_cols = as.logical,
      int_cols = as.integer,
      double_cols = as.double,
      factor_cols = function(x) {
        if (is.factor(x)) return(x)  # Already a factor
        if (drop_unused_levels) {
          factor(x, ordered = ordered_factors)
        } else {
          addNA(factor(x, ordered = ordered_factors), ifany = TRUE)
        }
      }
    )

    # Custom factor levels if provided
    if (!is.null(factor_levels)) {
      type_handlers[["factor_cols"]] <- function(x) {
        if (is.factor(x)) {
          levels(x) <- factor_levels[[cur_col]]  # cur_col set in the loop
          return(x)
        }
        factor(x, levels = factor_levels[[cur_col]], ordered = ordered_factors)
      }
    }

    # Validate columns
    missing_cols <- setdiff(unlist(cols_list), names(dt))
    if (length(missing_cols)) {
      warning("Missing columns: ", paste(missing_cols, collapse = ", "))
      cols_list <- lapply(cols_list, function(x) intersect(x, names(dt)))
    }

    # Apply conversions
    for (type in names(type_handlers)) {
      if (!is.null(cols_list[[type]])) {
        if (type == "factor_cols" && !is.null(factor_levels)) {
          for (cur_col in cols_list[[type]]) {
            dt[, (cur_col) := type_handlers[[type]](get(cur_col))]
          }
        } else {
          dt[, (cols_list[[type]]) := lapply(.SD, type_handlers[[type]]),
             .SDcols = cols_list[[type]]]
        }
      }
    }

    invisible(dt)
}


#' Fetch and Read Google Drive Files ----
#'
#' @param file_pattern Pattern to search for in Google Drive
#' @param key_path Path to service account JSON key file
#' @param key_file Name of the JSON key file
#' @param data_path Local path to save downloaded files
#' @param read_file Should the file be read into memory? (TRUE/FALSE)
#' @param max_days_old Maximum age of file in days to consider valid (NULL to ignore)
#' @param n_skip Number of lines to skip when reading (passed to fread)
#' @param default_id Fallback Drive ID if pattern matches fail
#' @param verbose Print progress messages? (TRUE/FALSE)
#' @return Either the file path or a data.table, depending on read_file
#' @examples
#' # Basic usage
#' data <- drive_fetch_read("data_goods_new.csv",
#'                         key_path = "keys/",
#'                         key_file = "57.json")
#'
fetchDriveData <- function(filePattern = NULL, id = NULL, keyFile, keyPath = "keys/",
                           dataPath = "data/", readFile = TRUE, autoSelect = "newest",
                           checkMetadata = TRUE, verbose = TRUE,
                           conflictAction = c("rename", "overwrite", "skip", "error"),
                           ...) {

  # --- 1. Package Validation ---
  if (!requireNamespace("googledrive", quietly = TRUE)) {
    stop("Required package 'googledrive' not installed. Install with: install.packages('googledrive')")
  }
  if (readFile && !requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' required for readFile=TRUE. Install with: install.packages('data.table')")
  }

  # --- 2. Parameter Validation ---
  if (is.null(filePattern) && is.null(id)) {
    stop("Must provide either filePattern or id")
  }
  conflictAction <- match.arg(conflictAction)
  validSelections <- c("newest", "oldest", "first")
  autoSelect <- match.arg(autoSelect, validSelections)

  # --- 3. Authentication & Setup ---
  googledrive::drive_auth(path = file.path(keyPath, keyFile))
  if (verbose) writeLines(paste("Authenticated with:", keyFile))

  # --- 4. File Retrieval ---
  targetFile <- if (!is.null(id)) {
    if (verbose) writeLines(paste("Accessing by ID:", id))
    googledrive::drive_get(id = id) |> data.table::as.data.table()
  } else {
    if (verbose) writeLines(paste("Searching for:", filePattern))
    allFiles <- googledrive::drive_find() |> data.table::as.data.table()
    matchedFiles <- allFiles[name %ilike% filePattern]

    if (nrow(matchedFiles) == 0) stop("No files matched pattern: ", filePattern)

    switch(autoSelect,
           "newest" = matchedFiles[which.max(as.POSIXct(sapply(drive_resource, `[[`, "modifiedTime")))],
           "oldest" = matchedFiles[which.min(as.POSIXct(sapply(drive_resource, `[[`, "modifiedTime")))],
           "first"  = {
             if (verbose) writeLines("Selecting first match by index")
             matchedFiles[1]
           }
    )
  }

  # --- 5. Precision Metadata ---
  resource <- targetFile[, drive_resource][[1]]
  modifiedTime <- format(as.POSIXct(resource$modifiedTime), "%Y-%m-%d %H:%M:%S")
  createdTime <- format(as.POSIXct(resource$createdTime), "%Y-%m-%d %H:%M:%S")

  if (checkMetadata && verbose) {
    writeLines("\nFile Metadata:")
    writeLines(paste0(
      "  Name: ", resource$name, "\n",
      "  ID: ", resource$id, "\n",
      "  Created: ", createdTime, "\n",
      "  Modified: ", modifiedTime, "\n",
      "  Size: ", format(structure(as.numeric(resource$size), class = "object_size"), "\n",
                         "  Owner: ", resource$owners[[1]]$displayName
      )))
  }

  # --- 6. Conflict Resolution & Download ---
  localPath <- file.path(dataPath, targetFile[, name])

  if (file.exists(localPath)) {
    switch(conflictAction,
           "rename" = {
             fileExt <- tools::file_ext(targetFile[, name])
             baseName <- tools::file_path_sans_ext(targetFile[, name])
             timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
             newFilename <- paste0(baseName, "_", timestamp, ifelse(nchar(fileExt) > 0, paste0(".", fileExt), ""))
             localPath <- file.path(dataPath, newFilename)
             if (verbose) writeLines(paste("Renaming to avoid conflict:", newFilename))
           },
           "overwrite" = {
             if (verbose) writeLines("Overwriting existing file by user request")
           },
           "skip" = {
             if (verbose) writeLines("File exists - skipping download")
             return(if (readFile) data.table::fread(localPath, ...) else localPath)
           },
           "error" = stop("File already exists at: ", localPath)
    )
  }

  if (verbose) writeLines(paste("\nDownloading to:", localPath))
  googledrive::drive_download(
    file = googledrive::as_id(targetFile[, id]),
    overwrite = (conflictAction == "overwrite"),
    path = localPath
  )

  # --- 7. Safe File Reading ---
  if (readFile) {
    fileExt <- tolower(tools::file_ext(resource$name))
    textMimes <- c("text/csv", "text/plain", "text/tab-separated-values")

    if (!resource$mimeType %in% textMimes) {
      stop(paste(
        "Unsupported file type:", resource$mimeType,
        "\nUse readFile = FALSE to download only",
        "\nSupported types:", paste(textMimes, collapse = ", ")
      ))
    }

    if (verbose) writeLines(paste("Reading", fileExt, "file with fread()"))
    args <- list(
      file = localPath,
      sep = if (fileExt == "csv") ";" else "auto",
      header = FALSE,
      fill = TRUE,
      colClasses = "character",
      quote = "",
      ...
    )
    do.call(data.table::fread, args)
  } else {
    if (verbose) writeLines("Returning file path only")
    localPath
  }
}




#' Safe Lapply with Progress
#' @param X Input list/vector
#' @param FUN Function to apply
#' @param .try Catch errors? (TRUE/FALSE)
#' @param .progress Show progress? (TRUE/"text"/FALSE)
#' @return data.table with results/errors
safe_apply <- function(X, FUN, ..., .try = TRUE, .progress = TRUE) {
  res <- data.table::data.table(
    id = seq_along(X),
    input = if (is.list(X)) I(X) else X,
    success = NA,
    result = vector("list", length(X)))

    for (i in seq_along(X)) {
      if (!identical(.progress, FALSE)) {
        msg <- if (is.character(.progress)) .progress else "Processing"
        cat(sprintf("[%d/%d] %s: %s\n", i, length(X), msg, X[[i]]))
      }

      res$result[[i]] <- if (.try) {
        tryCatch(
          { res$success[i] <- TRUE; FUN(X[[i]], ...) },
          error = function(e) {
            res$success[i] <- FALSE
            paste0("Error: ", conditionMessage(e))
          }
        )
      } else {
        res$success[i] <- TRUE
        FUN(X[[i]], ...)
      }
    }

    if (.progress) cat("Done!\n")
    res
}

#  Add this to R-profile!
my_lapply <- function(X, FUN, ...) {
  invisible(safe_apply(X, FUN, ..., .progress = "Running"))
}


#' Parse command line arguments into a named list ----
#'
#' @param verbose Logical. If TRUE, prints argument processing details.
#' @param return_type Either "list" (default) or "data.table" for return type.
#' @return Named list or data.table of arguments. Returns NULL if no args.
#' @examples
#' # For command: Rscript script.R seed=42 debug=TRUE title="My Analysis"
#' # args <- getArgs()
#'
getArgs <- function(convert_types = TRUE) {
  args <- commandArgs(trailingOnly = TRUE)
  if (!length(args)) return(NULL)

  pairs <- strsplit(args, "=")
  values <- lapply(pairs, `[`, 2)
  names(values) <- vapply(pairs, `[`, 1, FUN.VALUE = character(1))

  if (convert_types) {
    values <- lapply(values, function(x) {
      if (x %in% c("TRUE","FALSE")) return(as.logical(x))
      num_val <- suppressWarnings(as.numeric(x))
      if (!is.na(num_val)) num_val else x
    })
  }

  values
}
