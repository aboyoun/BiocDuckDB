#' Common operations on DuckDBArraySeed objects
#'
#' @description
#' Common operations on \linkS4class{DuckDBArraySeed} objects.
#'
#' @section Group Generics:
#' DuckDBTable objects have support for S4 group generic functionality:
#' \describe{
#'   \item{\code{Arith}}{\code{"+"}, \code{"-"}, \code{"*"}, \code{"^"},
#'     \code{"\%\%"}, \code{"\%/\%"}, \code{"/"}}
#'   \item{\code{Compare}}{\code{"=="}, \code{">"}, \code{"<"}, \code{"!="},
#'     \code{"<="}, \code{">="}}
#'   \item{\code{Logic}}{\code{"&"}, \code{"|"}}
#'   \item{\code{Ops}}{\code{"Arith"}, \code{"Compare"}, \code{"Logic"}}
#'   \item{\code{Math}}{\code{"abs"}, \code{"sign"}, \code{"sqrt"},
#'     \code{"ceiling"}, \code{"floor"}, \code{"trunc"}, \code{"log"},
#'     \code{"log10"}, \code{"log2"}, \code{"acos"}, \code{"acosh"},
#'     \code{"asin"}, \code{"asinh"}, \code{"atan"}, \code{"atanh"},
#'     \code{"exp"}, \code{"expm1"}, \code{"cos"}, \code{"cosh"},
#'     \code{"sin"}, \code{"sinh"}, \code{"tan"}, \code{"tanh"},
#'     \code{"gamma"}, \code{"lgamma"}}
#'   \item{\code{Summary}}{\code{"max"}, \code{"min"}, \code{"range"},
#'     \code{"prod"}, \code{"sum"}, \code{"any"}, \code{"all"}}
#'  }
#'  See \link[methods]{S4groupGeneric} for more details.
#'
#' @section Numerical Data Methods:
#' In the code snippets below, \code{x} is a DuckDBArraySeed object:
#' \describe{
#'   \item{\code{is.finite(x)}:}{
#'     Returns a DuckDBArraySeed containing logicals that indicate which values
#'     are finite.
#'   }
#'   \item{\code{is.infinite(x)}:}{
#'     Returns a DuckDBArraySeed containing logicals that indicate which values
#'     are infinite.
#'   }
#'   \item{\code{is.nan(x)}:}{
#'     Returns a DuckDBArraySeed containing logicals that indicate which values
#'     are Not a Number.
#'   }
#'   \item{\code{rowSums(x, dims = 1)}:}{
#'     Calculates the row sums of \code{x}.
#'     \describe{
#'       \item{\code{dims}}{An integer specifying which dimensions to sum over,
#'         namely \code{dims + 1}, \ldots.}
#'     }
#'   }
#'   \item{\code{colSums(x, dims = 1)}:}{
#'     Calculates the column sums of \code{x}.
#'     \describe{
#'       \item{\code{dims}}{An integer specifying which dimensions to sum over,
#'         namely \code{1:dims}.}
#'     }
#'   }
#' }
#'
#' @section Sparisty Methods:
#' In the code snippets below, \code{x} is a DuckDBArraySeed object:
#' \describe{
#'   \item{\code{is_nonzero(x)}:}{
#'     Returns a DuckDBArraySeed containing logicals that indicate if the
#'     values in each of the columns of \code{x} are non-zero.
#'   }
#'   \item{\code{nzcount(x)}:}{
#'     Returns the total number of non-zero values.
#'   }
#'   \item{\code{is_sparse(x)}:}{
#'     Returns \code{TRUE} since data are stored in a sparse array representation.
#'   }
#' }
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
#' is_nonzero,DuckDBArraySeed-method
#' nzcount,DuckDBArraySeed-method
#' is_sparse,DuckDBArraySeed-method
#'
#' @include DuckDBArraySeed-class.R
#' @include DuckDBTable-utils.R
#'
#' @keywords utilities methods
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
