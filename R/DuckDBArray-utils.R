#' DuckDBArray Utilities
#'
#' @author Patrick Aboyoun
#'
#' @aliases
#' Ops,DuckDBArray,DuckDBArray-method
#' Ops,DuckDBArray,atomic-method
#' Ops,atomic,DuckDBArray-method
#' Math,DuckDBArray-method
#'
#' is.finite,DuckDBArray-method
#' is.infinite,DuckDBArray-method
#' is.nan,DuckDBArray-method
#' rowSums,DuckDBArray-method
#' colSums,DuckDBArray-method
#'
#' %in%,DuckDBArray,ANY-method
#'
#' is_nonzero,DuckDBArray-method
#' nzcount,DuckDBArray-method
#'
#' @include DuckDBArray.R
#' @include DuckDBTable-utils.R
#'
#' @name DuckDBArray-utils
NULL

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Group generic methods
###

#' @export
setMethod("Ops", c(e1 = "DuckDBArray", e2 = "DuckDBArray"), function(e1, e2) {
    replaceSlots(e1, seed = callGeneric(e1@seed, e2@seed), check = FALSE)
})

#' @export
setMethod("Ops", c(e1 = "DuckDBArray", e2 = "atomic"), function(e1, e2) {
    replaceSlots(e1, seed = callGeneric(e1@seed, e2), check = FALSE)
})

#' @export
setMethod("Ops", c(e1 = "atomic", e2 = "DuckDBArray"), function(e1, e2) {
    replaceSlots(e2, seed = callGeneric(e1, e2@seed), check = FALSE)
})

#' @export
setMethod("Math", "DuckDBArray", function(x) {
    replaceSlots(x, seed = callGeneric(x@seed), check = FALSE)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Numerical methods
###

#' @export
setMethod("is.finite", "DuckDBArray", function(x) {
    replaceSlots(x, seed = callGeneric(x@seed), check = FALSE)
})

#' @export
setMethod("is.infinite", "DuckDBArray", function(x) {
    replaceSlots(x, seed = callGeneric(x@seed), check = FALSE)
})

#' @export
setMethod("is.nan", "DuckDBArray", function(x) {
    replaceSlots(x, seed = callGeneric(x@seed), check = FALSE)
})

#' @export
#' @importFrom DelayedArray rowSums
setMethod("rowSums", "DuckDBArray", function(x, na.rm = FALSE, dims = 1, ...) {
    as.array(replaceSlots(x, seed = callGeneric(x@seed, na.rm = na.rm, dims = dims, ...), check = FALSE), drop = TRUE)
})

#' @export
#' @importFrom DelayedArray colSums
setMethod("colSums", "DuckDBArray", function(x, na.rm = FALSE, dims = 1, ...) {
    as.array(replaceSlots(x, seed = callGeneric(x@seed, na.rm = na.rm, dims = dims, ...), check = FALSE), drop = TRUE)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Set methods
###

#' @export
#' @importFrom BiocGenerics %in%
setMethod("%in%", c(x = "DuckDBArray", table = "ANY"), function(x, table) {
    replaceSlots(x, seed = callGeneric(x@seed, table), check = FALSE)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Sparsity methods
###

#' @export
#' @importFrom SparseArray is_nonzero
setMethod("is_nonzero", "DuckDBArray", function(x) {
    replaceSlots(x, seed = callGeneric(x@seed), check = FALSE)
})

#' @export
#' @importFrom SparseArray nzcount
setMethod("nzcount", "DuckDBArray", function(x) {
    callGeneric(x@seed)
})
