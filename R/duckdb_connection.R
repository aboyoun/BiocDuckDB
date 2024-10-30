#' DuckDB Connection Accessor
#'
#' Get the DuckDB connection value contained in an object.
#'
#' @param x An object to get the DuckDB connection value.
#' @param ... Additional arguments, for use in specific methods.
#'
#' @author Patrick Aboyoun
#'
#' @examples
#' duckdb_connection
#' showMethods("duckdb_connection")
#'
#' @aliases
#' duckdb_connection
#'
#' duckdb_connection,ParquetArray-method
#' duckdb_connection,ParquetArraySeed-method
#' duckdb_connection,ParquetColumn-method
#' duckdb_connection,ParquetFactTable-method
#' duckdb_connection,ParquetMatrix-method
#'
#' @export
setGeneric("duckdb_connection", function(x, ...) standardGeneric("duckdb_connection"))

setOldClass("tbl_duckdb_connection")

.getColumnType <- function(column) {
    if (inherits(column, "Date")) {
        "Date"
    } else {
        DelayedArray::type(column)
    }
}
