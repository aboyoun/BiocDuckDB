persistent <- new.env()
persistent$handles <- list()

#' Acquire the DuckDB Table Connection
#'
#' Acquire a (possibly cached) DuckDB table connection.
#'
#' @param path String specifying a path to a data directory or file.
#' @param ... Further arguments to be passed to \code{read_parquet}.
#'
#' @return
#' For \code{acquireTable}, an \code{tbl_duckdb_connection} identical to that returned by \code{tbl(dbConnect(), src)}.
#'
#' For \code{releaseTable}, any existing \code{tbl_duckdb_connection} for the \code{path} is cleared from cache, and \code{NULL} is invisibly returned.
#' If \code{path=NULL}, all cached DuckDB table connections are removed.
#'
#' @author Aaron Lun, Patrick Aboyoun
#'
#' @details
#' \code{acquireTable} will cache the \code{tbl_duckdb_connection} object in the current R session to avoid repeated initialization.
#' This improves efficiency for repeated calls, e.g., when creating a \linkS4class{DataFrame} with multiple columns from the same data path.
#' The cached DuckDB table connection for any given \code{path} can be deleted by calling \code{releaseTable} for the same \code{path}.
#'
#' @examples
#' # Mocking up a parquet file:
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' arrow::write_parquet(mtcars, tf)
#'
#' acquireTable(tf)
#' acquireTable(tf) # just re-uses the cache
#' releaseTable(tf) # clears the cache
#'
#' # Mocking up a parquet data diretory:
#' td <- tempfile()
#' on.exit(unlink(td), add = TRUE)
#' arrow::write_dataset(mtcars, td, format = "parquet", partitioning = "cyl")
#'
#' acquireTable(td)
#' acquireTable(td) # just re-uses the cache
#' releaseTable(td) # clears the cache
#' @export
#' @importFrom DBI dbConnect
#' @importFrom dplyr tbl
#' @importFrom duckdb duckdb
#' @importFrom S4Vectors isSingleString isTRUEorFALSE tail
acquireTable <- function(path, ...) {
    if (!(isSingleString(path) && nzchar(path))) {
        stop("'path' must be a single non-empty string")
    }

    # Here we set up an LRU cache for the data handles.
    # This avoids the initialization time when reading lots of columns.
    nhandles <- length(persistent$handles)

    i <- which(names(persistent$handles) == path)
    if (length(i)) {
        output <- persistent$handles[[i]]
        if (i < nhandles) {
            persistent$handles <- persistent$handles[c(seq_len(i-1L), seq(i+1L, nhandles), i)] # moving to the back
        }
        return(output)
    }

    # Pulled this value out of my ass.
    limit <- 100
    if (nhandles >= limit) {
        persistent$handles <- tail(persistent$handles, limit - 1L)
    }

    args <- list(...)
    if (length(args) == 0L) {
        args <- ""
    } else {
        args <- lapply(args, function(x) {
            if (isTRUEorFALSE(x)) ifelse(x, "true", "false") else x
        })
        args <- paste(c("", paste(names(args), args, sep = " = ")), collapse = ", ")
    }
    if (dir.exists(path)) {
        src <- sprintf("read_parquet('%s'%s)", file.path(path, "**"), args)
    } else {
        src <- sprintf("read_parquet('%s'%s)", path, args)
    }
    output <- tbl(dbConnect(duckdb(bigint = "integer64")), src)
    persistent$handles[[path]] <- output
    output
}

#' @export
#' @rdname acquireTable
#' @importFrom DBI dbDisconnect
releaseTable <- function(path) {
    if (is.null(path)) {
        persistent$handles <- list()
    } else {
        i <- which(names(persistent$handles) == path)
        if (length(i)) {
            dbDisconnect(persistent$handles[[i]]$src$con)
            persistent$handles <- persistent$handles[-i]
        }
    }
    invisible(NULL)
}
