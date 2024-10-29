#' Arrow Query Accessor
#'
#' Get the arrow query value contained in an object.
#'
#' @param x An object to get the arrow query value.
#' @param ... Additional arguments, for use in specific methods.
#'
#' @author Patrick Aboyoun
#'
#' @examples
#' arrow_query
#' showMethods("arrow_query")
#'
#' @aliases
#' arrow_query
#'
#' arrow_query,ParquetArray-method
#' arrow_query,ParquetArraySeed-method
#' arrow_query,ParquetColumn-method
#' arrow_query,ParquetFactTable-method
#' arrow_query,ParquetMatrix-method
#'
#' @export
setGeneric("arrow_query", function(x, ...) standardGeneric("arrow_query"))

setOldClass("arrow_dplyr_query")

#' @importFrom dplyr pull slice_head
.getColumnType <- function(column_query) {
    DelayedArray::type(pull(slice_head(column_query, n = 0L), as_vector = TRUE))
}
