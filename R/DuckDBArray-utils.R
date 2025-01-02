#' Common operations on DuckDBArray objects
#'
#' @description
#' Common operations on \linkS4class{DuckDBArray} objects.
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
#' In the code snippets below, \code{x} is a DuckDBArray object:
#' \describe{
#'   \item{\code{is.finite(x)}:}{
#'     Returns a DuckDBArray containing logicals that indicate which values are
#'     finite.
#'   }
#'   \item{\code{is.infinite(x)}:}{
#'     Returns a DuckDBArray containing logicals that indicate which values are
#'     infinite.
#'   }
#'   \item{\code{is.nan(x)}:}{
#'     Returns a DuckDBArray containing logicals that indicate which values are
#'     Not a Number.
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
#' In the code snippets below, \code{x} is a DuckDBArray object:
#' \describe{
#'   \item{\code{is_nonzero(x)}:}{
#'     Returns a DuckDBArray containing logicals that indicate if the values in
#'     each of the columns of \code{x} are non-zero.
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
#' is_nonzero,DuckDBArray-method
#' nzcount,DuckDBArray-method
#'
#' @include DuckDBArray-class.R
#' @include DuckDBTable-utils.R
#'
#' @keywords utilities methods
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
