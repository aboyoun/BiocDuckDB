.duckdb <- new.env()
.duckdb$drv <- NULL

#' @importFrom DBI dbDisconnect
reg.finalizer(.duckdb, function(env) {
    if (!is.null(env$drv)) {
        removeDuckDBConn()
  }
}, onexit = TRUE)

#' Acquire and Release the DuckDB Connection
#'
#' Acquire and Release the DuckDB connection used by the
#' \pkg{BiocDuckDB} package.
#'
#' @param conn A \code{duckdb_connection} object.
#'
#' @return
#' \code{acquireDuckDBConn} returns the current DuckDB connection.
#'
#' @author Patrick Aboyoun
#'
#' @details
#' \code{acquireDuckDBConn} will add a \code{duckdb_connection} object to
#' the \pkg{BiocDuckDB} package cache.
#' \code{releaseDuckDBConn} will remove the \code{duckdb_connection} object
#' from that cache.
#'
#' @examples
#' \dontrun{
#' acquireDuckDBConn()
#' releaseDuckDBConn()
#' }
#'
#' @aliases
#' acquireDuckDBConn
#' releaseDuckDBConn
#'
#' @keywords IO
#'
#' @name DuckDBConnection
NULL

#' @export
#' @importFrom DBI dbConnect dbDisconnect
#' @importFrom duckdb duckdb
#' @importFrom duckdbfs load_spatial
#' @rdname DuckDBConnection
acquireDuckDBConn <- function(conn = dbConnect(duckdb(bigint = "integer64"))) {
    if (is.null(.duckdb$drv)) {
        if (!inherits(conn, "duckdb_connection")) {
            stop("'conn' must be a DuckDB connection")
        }
        load_spatial(conn)
        .duckdb$drv <- conn
    }
    .duckdb$drv
}

#' @export
#' @importFrom DBI dbDisconnect
#' @rdname DuckDBConnection
releaseDuckDBConn <- function() {
    if (!is.null(.duckdb$drv)) {
        dbDisconnect(.duckdb$drv)
        .duckdb$drv <- NULL
    }
    invisible(NULL)
}
