#' DuckDB-backed DataFrameList
#'
#' @description
#' Create a DuckDB-backed \linkS4class{DataFrameList} object.
#'
#' @author Patrick Aboyoun
#'
#' @aliases
#' DuckDBDataFrameList-class
#' split,DuckDBDataFrame,DuckDBColumn-method
#'
#' @include DuckDBDataFrame-class.R
#' @include DuckDBList-class.R
#'
#' @name DuckDBDataFrameList-class
NULL

#' @export
#' @importClassesFrom IRanges SplitDataFrameList
setClass("DuckDBDataFrameList", contains = c("SplitDataFrameList", "DuckDBList"),
         prototype = prototype(elementType = "DuckDBDataFrame", unlistData = new("DuckDBDataFrame")))

#' @export
#' @importFrom S4Vectors split
#' @importFrom stats setNames
setMethod("split", c("DuckDBDataFrame", "DuckDBColumn"), function(x, f, drop = FALSE, ...) {
    if (!isTRUE(all.equal(as(x, "DuckDBTable"), f@table))) {
        stop("cannot split a DuckDBDataFrame object by an incompatible DuckDBColumn object")
    }
    elementNROWS <- table(f)
    elementNROWS <- setNames(as.vector(elementNROWS), names(elementNROWS))
    new2("DuckDBDataFrameList", unlistData = x, partitioning = f@table@datacols,
         names = names(elementNROWS), elementNROWS = elementNROWS, check = FALSE)
})
