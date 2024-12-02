#' DuckDBColumn Utilities
#'
#' @author Patrick Aboyoun
#'
#' @aliases
#' Ops,DuckDBColumn,DuckDBColumn-method
#' Ops,DuckDBColumn,atomic-method
#' Ops,atomic,DuckDBColumn-method
#' Math,DuckDBColumn-method
#' Summary,DuckDBColumn-method
#'
#' is.finite,DuckDBColumn-method
#' is.infinite,DuckDBColumn-method
#' is.nan,DuckDBColumn-method
#' mean,DuckDBColumn-method
#' var,DuckDBColumn,ANY-method
#' sd,DuckDBColumn-method
#' median.DuckDBColumn
#' quantile.DuckDBColumn
#' mad,DuckDBColumn-method
#' IQR,DuckDBColumn-method
#'
#' unique,DuckDBColumn-method
#' %in%,DuckDBColumn,ANY-method
#' table,DuckDBColumn-method
#'
#' is_nonzero,DuckDBColumn-method
#' nzcount,DuckDBColumn-method
#'
#' @include DuckDBColumn.R
#'
#' @name DuckDBColumn-utils
NULL

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Group generic methods
###

#' @export
setMethod("Ops", c(e1 = "DuckDBColumn", e2 = "DuckDBColumn"), function(e1, e2) {
    replaceSlots(e1, table = callGeneric(e1@table, e2@table), check = FALSE)
})

#' @export
setMethod("Ops", c(e1 = "DuckDBColumn", e2 = "atomic"), function(e1, e2) {
    replaceSlots(e1, table = callGeneric(e1@table, e2), check = FALSE)
})

#' @export
setMethod("Ops", c(e1 = "atomic", e2 = "DuckDBColumn"), function(e1, e2) {
    replaceSlots(e2, table = callGeneric(e1, e2@table), check = FALSE)
})

#' @export
setMethod("Math", "DuckDBColumn", function(x) {
    replaceSlots(x, table = callGeneric(x@table), check = FALSE)
})

#' @export
setMethod("Summary", "DuckDBColumn", function(x, ..., na.rm = FALSE) {
    callGeneric(x@table)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Numerical methods
###

#' @export
setMethod("is.finite", "DuckDBColumn", function(x) {
    replaceSlots(x, table = callGeneric(x@table), check = FALSE)
})

#' @export
setMethod("is.infinite", "DuckDBColumn", function(x) {
    replaceSlots(x, table = callGeneric(x@table), check = FALSE)
})

#' @export
setMethod("is.nan", "DuckDBColumn", function(x) {
    replaceSlots(x, table = callGeneric(x@table), check = FALSE)
})

#' @export
#' @importFrom BiocGenerics mean
setMethod("mean", "DuckDBColumn", function(x, ...) {
    callGeneric(x@table)
})

#' @export
#' @importFrom BiocGenerics var
setMethod("var", "DuckDBColumn", function(x, y = NULL, na.rm = FALSE, use)  {
    callGeneric(x@table)
})

#' @export
#' @importFrom BiocGenerics sd
setMethod("sd", "DuckDBColumn", function(x, na.rm = FALSE) {
    callGeneric(x@table)
})

#' @exportS3Method stats::median
#' @importFrom stats median
median.DuckDBColumn <- function(x, na.rm = FALSE, ...) {
    median(x@table, na.rm = na.rm, ...)
}

#' @exportS3Method stats::quantile
#' @importFrom stats quantile
quantile.DuckDBColumn <-
function(x, probs = seq(0, 1, 0.25), na.rm = FALSE, names = TRUE, type = 7, digits = 7, ...) {
    quantile(x@table, probs = probs, na.rm = na.rm, names = names, type = type, digits = digits, ...)
}

#' @export
#' @importFrom BiocGenerics mad
setMethod("mad", "DuckDBColumn",
function(x, center = median(x), constant = 1.4826, na.rm = FALSE, low = FALSE, high = FALSE) {
    callGeneric(x@table, constant = constant)
})

#' @export
#' @importFrom BiocGenerics IQR
setMethod("IQR", "DuckDBColumn", function(x, na.rm = FALSE, type = 7) {
    callGeneric(x@table, type = type)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Set methods
###

#' @export
#' @importFrom BiocGenerics unique
setMethod("unique", "DuckDBColumn",
function (x, incomparables = FALSE, fromLast = FALSE, ...)  {
    replaceSlots(x, table = callGeneric(x@table), check = FALSE)
})

#' @export
#' @importFrom BiocGenerics %in%
setMethod("%in%", c(x = "DuckDBColumn", table = "ANY"), function(x, table) {
    replaceSlots(x, table = callGeneric(x@table, table), check = FALSE)
})

#' @export
#' @importFrom BiocGenerics table
setMethod("table", "DuckDBColumn", function(...) {
    callGeneric(cbind.DuckDBDataFrame(...))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Sparsity methods
###

#' @export
#' @importFrom SparseArray is_nonzero
setMethod("is_nonzero", "DuckDBColumn", function(x) {
    replaceSlots(x, table = callGeneric(x@table), check = FALSE)
})

#' @export
#' @importFrom SparseArray nzcount
setMethod("nzcount", "DuckDBColumn", function(x) {
    callGeneric(x@table)
})
