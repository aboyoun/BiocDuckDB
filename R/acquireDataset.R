persistent <- new.env()
persistent$handles <- list()

#' Acquire the Arrow Dataset
#'
#' Acquire a (possibly cached) Arrow Dataset created from Parquet data.
#'
#' @param path String specifying a path to a Parquet data directory or file.
#' @param ... Further arguments to be passed to \code{read_parquet}.
#'
#' @return
#' For \code{acquireDataset}, an Arrow Dataset identical to that returned by \code{as_arrow_table(\link[arrow]{open_dataset})}.
#'
#' For \code{releaseDataset}, any existing Dataset for the \code{path} is cleared from cache, and \code{NULL} is invisibly returned.
#' If \code{path=NULL}, all cached Datasets are removed.
#'
#' @author Aaron Lun, Patrick Aboyoun
#'
#' @details
#' \code{acquireDataset} will cache the Dataset object in the current R session to avoid repeated initialization.
#' This improves efficiency for repeated calls, e.g., when creating a \linkS4class{DataFrame} with multiple columns from the same Parquet data path.
#' The cached Dataset for any given \code{path} can be deleted by calling \code{releaseDataset} for the same \code{path}.
#'
#' @examples
#' # Mocking up a parquet file:
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' arrow::write_parquet(mtcars, tf)
#'
#' acquireDataset(tf)
#' acquireDataset(tf) # just re-uses the cache
#' releaseDataset(tf) # clears the cache
#'
#' # Mocking up a parquet data diretory:
#' td <- tempfile()
#' on.exit(unlink(td), add = TRUE)
#' arrow::write_dataset(mtcars, td, format = "parquet", partitioning = "cyl")
#'
#' acquireDataset(td)
#' acquireDataset(td) # just re-uses the cache
#' releaseDataset(td) # clears the cache
#' @export
#' @importFrom DBI dbConnect
#' @importFrom dplyr tbl
#' @importFrom duckdb duckdb
#' @importFrom S4Vectors isSingleString isTRUEorFALSE tail
acquireDataset <- function(path, ...) {
    if (!(isSingleString(path) && nzchar(path))) {
        stop("'path' must be a single non-empty string")
    }

    # Here we set up an LRU cache for the Parquet handles.
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
    output <- tbl(dbConnect(duckdb()), src)
    persistent$handles[[path]] <- output
    output
}

#' @export
#' @rdname acquireDataset
#' @importFrom DBI dbDisconnect
releaseDataset <- function(path) {
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
