#' DuckDBArraySeed Utilities
#'
#' @author Patrick Aboyoun
#'
#' @aliases
#' Ops,DuckDBArraySeed,DuckDBArraySeed-method
#' Ops,DuckDBArraySeed,atomic-method
#' Ops,atomic,DuckDBArraySeed-method
#' Math,DuckDBArraySeed-method
#'
#' is.finite,DuckDBArraySeed-method
#' is.infinite,DuckDBArraySeed-method
#' is.nan,DuckDBArraySeed-method
#' rowSums,DuckDBArraySeed-method
#' colSums,DuckDBArraySeed-method
#'
#' %in%,DuckDBArraySeed,ANY-method
#'
#' is_nonzero,DuckDBArraySeed-method
#' nzcount,DuckDBArraySeed-method
#' is_sparse,DuckDBArraySeed-method
#'
#' @include DuckDBArraySeed-class.R
#' @include DuckDBTable-utils.R
#'
#' @name DuckDBArraySeed-utils
NULL

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Group generic methods
###

#' @export
setMethod("Ops", c(e1 = "DuckDBArraySeed", e2 = "DuckDBArraySeed"), function(e1, e2) {
    if (!isTRUE(all.equal(e1@table, e2@table)) || !identical(e1@drop, e2@drop)) {
        stop("can only perform binary operations with compatible objects")
    }
    replaceSlots(e1, table = callGeneric(e1@table, e2@table), check = FALSE)
})

#' @export
setMethod("Ops", c(e1 = "DuckDBArraySeed", e2 = "atomic"), function(e1, e2) {
    replaceSlots(e1, table = callGeneric(e1@table, e2), check = FALSE)
})

#' @export
setMethod("Ops", c(e1 = "atomic", e2 = "DuckDBArraySeed"), function(e1, e2) {
    replaceSlots(e2, table = callGeneric(e1, e2@table), check = FALSE)
})

#' @export
setMethod("Math", "DuckDBArraySeed", function(x) {
    replaceSlots(x, table = callGeneric(x@table), check = FALSE)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Numerical methods
###

#' @export
setMethod("is.finite", "DuckDBArraySeed", function(x) {
    replaceSlots(x, table = callGeneric(x@table), check = FALSE)
})

#' @export
setMethod("is.infinite", "DuckDBArraySeed", function(x) {
    replaceSlots(x, table = callGeneric(x@table), check = FALSE)
})

#' @export
setMethod("is.nan", "DuckDBArraySeed", function(x) {
    replaceSlots(x, table = callGeneric(x@table), check = FALSE)
})

#' @export
#' @importFrom DelayedArray rowSums
setMethod("rowSums", "DuckDBArraySeed", function(x, na.rm = FALSE, dims = 1, ...) {
    replaceSlots(x, table = callGeneric(x@table, na.rm = na.rm, dims = dims, ...), check = FALSE)
})

#' @export
#' @importFrom DelayedArray colSums
setMethod("colSums", "DuckDBArraySeed", function(x, na.rm = FALSE, dims = 1, ...) {
    replaceSlots(x, table = callGeneric(x@table, na.rm = na.rm, dims = dims, ...), check = FALSE)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Set methods
###

#' @export
#' @importFrom BiocGenerics %in%
setMethod("%in%", c(x = "DuckDBArraySeed", table = "ANY"), function(x, table) {
    replaceSlots(x, table = callGeneric(x@table, table), check = FALSE)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Sparsity methods
###

#' @export
#' @importFrom SparseArray is_nonzero
setMethod("is_nonzero", "DuckDBArraySeed", function(x) {
    replaceSlots(x, table = callGeneric(x@table), check = FALSE)
})

#' @export
#' @importFrom SparseArray nzcount
setMethod("nzcount", "DuckDBArraySeed", function(x) {
    callGeneric(x@table)
})
#' @export
#' @importFrom S4Arrays is_sparse
setMethod("is_sparse", "DuckDBArraySeed", function(x) {
    callGeneric(x@table)
})
