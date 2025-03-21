#' Common operations on DuckDBColumn objects
#'
#' @description
#' Common operations on \linkS4class{DuckDBColumn} objects.
#'
#' @section Group Generics:
#' DuckDBColumn objects have support for S4 group generic functionality:
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
#' In the code snippets below, \code{x} is a DuckDBColumn object:
#' \describe{
#'   \item{\code{is.finite(x)}:}{
#'     Returns a DuckDBColumn containing logicals that indicate which values are
#'     finite.
#'   }
#'   \item{\code{is.infinite(x)}:}{
#'     Returns a DuckDBColumn containing logicals that indicate which values are
#'     infinite.
#'   }
#'   \item{\code{is.nan(x)}:}{
#'     Returns a DuckDBColumn containing logicals that indicate which values are
#'     Not a Number.
#'   }
#'   \item{\code{mean(x)}:}{
#'     Calculates the mean of \code{x}.
#'   }
#'   \item{\code{var(x)}:}{
#'     Calculates the variance of \code{x}.
#'   }
#'   \item{\code{sd(x)}:}{
#'     Calculates the standard deviation of \code{x}.
#'   }
#'   \item{\code{median(x)}:}{
#'     Calculates the median of \code{x}.
#'   }
#'   \item{\code{quantile(x, probs = seq(0, 1, 0.25), names = TRUE, type = 7)}:}{
#'     Calculates the specified quantiles of \code{x}.
#'     \describe{
#'       \item{\code{probs}}{A numeric vector of probabilities with values in
#'         [0,1].}
#'       \item{\code{names}}{If \code{TRUE}, the result has names describing the
#'         quantiles.}
#'       \item{\code{type}}{Either 1 or 7 that specifies the quantile algorithm
#'         detailed in \code{\link[stats]{quantile}}.}
#'     }
#'   }
#'   \item{\code{mad(x, constant = 1.4826)}:}{
#'     Calculates the median absolute deviation of \code{x}.
#'     \describe{
#'       \item{\code{constant}}{The scale factor.}
#'     }
#'   }
#'   \item{\code{IQR(x, type = 7)}:}{
#'     Calculates the interquartile range of \code{x}.
#'     \describe{
#'       \item{\code{type}}{Either 1 or 7 that specifies the quantile algorithm
#'         detailed in \code{\link[stats]{quantile}}.}
#'     }
#'   }
#' }
#'
#' @section General Methods:
#' In the code snippets below, \code{x} is a DuckDBColumn object:
#' \describe{
#'   \item{\code{unique(x)}:}{
#'     Returns a DuckDBColumn containing the distinct rows.
#'   }
#'   \item{\code{x \%in\% table}:}{
#'     Returns a DuckDBColumn containing logicals that indicate the elements of
#'     \code{x} in \code{table}.
#'   }
#'   \item{\code{table(...)}:}{
#'     Returns a table containing the counts across the distinct values.
#'   }
#' }
#'
#' @section Sparisty Methods:
#' In the code snippets below, \code{x} is a DuckDBColumn object:
#' \describe{
#'   \item{\code{is_nonzero(x)}:}{
#'     Returns a DuckDBColumn containing logicals that indicate if the elements
#'     of \code{x} are non-zero.
#'   }
#'   \item{\code{nzcount(x)}:}{
#'     Returns the total number of non-zero values.
#'   }
#' }
#'
#' @section Spatial Methods:
#' In the code snippets below, \code{x} is a DuckDBColumn object:
#' \describe{
#'   \item{\code{st_area(x)}:}{
#'     Returns a DuckDBColumn containing the areas of geometries.
#'   }
#'   \item{\code{st_as_binary(x, hex = FALSE)}:}{
#'     Returns the DuckDBColumn containing either WKB if \code{hex = FALSE} or
#'     HEXWKB if \code{hex = TRUE} representations of geometries.
#'   }
#'   \item{\code{st_as_text(x, geojson = FALSE)}:}{
#'     Returns the DuckDBColumn containing either WKT if \code{geojson = FALSE} or
#'     GeoJSON if \code{geojson = TRUE} representations of geometries.
#'   }
#'   \item{\code{st_boundary(x)}:}{
#'     Returns a DuckDBColumn containing the boundaries of geometries.
#'   }
#'   \item{\code{st_centroid(x)}:}{
#'     Returns a DuckDBColumn containing the centroids of geometries.
#'   }
#'   \item{\code{st_convex_hull(x)}:}{
#'     Returns a DuckDBColumn containing the convex hulls of geometries.
#'   }
#'   \item{\code{st_exterior_ring(x)}:}{
#'     Returns a DuckDBColumn containing the exterior rings of geometries.
#'   }
#'   \item{\code{st_is_valid(x)}:}{
#'     Returns a DuckDBColumn containing logicals that indicate if the
#'     geometries are valid.
#'   }
#'   \item{\code{st_line_merge(x, directed = FALSE)}:}{
#'     Returns a DuckDBColumn containing the merged lines of geometries,
#'     optionally taking direction into account.
#'   }
#'   \item{\code{st_make_valid(x)}:}{
#'     Returns a DuckDBColumn containing valid geometries.
#'   }
#'   \item{\code{st_normalize(x)}:}{
#'     Returns a DuckDBColumn containing normalized geometries.
#'   }
#'   \item{\code{st_point_on_surface(x)}:}{
#'     Returns a DuckDBColumn containing a point on the surface of the input
#'     geometry.
#'   }
#'   \item{\code{st_reverse(x)}:}{
#'     Returns a DuckDBColumn containing geometries with the vertice order
#'     reversed.
#'   }
#' }
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
#' st_area.DuckDBColumn
#' st_as_binary.DuckDBColumn
#' st_as_text.DuckDBColumn
#' st_boundary.DuckDBColumn
#' st_centroid.DuckDBColumn
#' st_convex_hull.DuckDBColumn
#' st_exterior_ring.DuckDBColumn
#' st_is_valid.DuckDBColumn
#' st_line_merge.DuckDBColumn
#' st_make_valid.DuckDBColumn
#' st_normalize.DuckDBColumn
#' st_point_on_surface.DuckDBColumn
#' st_reverse.DuckDBColumn
#'
#' @include DuckDBColumn-class.R
#' @include DuckDBTable-utils.R
#'
#' @keywords utilities methods
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


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Spatial methods
###

#' @exportS3Method sf::st_area
#' @importFrom sf st_area
st_area.DuckDBColumn <- function(x, ...) {
    replaceSlots(x, table = st_area(x@table), check = FALSE)
}

#' @exportS3Method sf::st_as_binary
#' @importFrom sf st_as_binary
st_as_binary.DuckDBColumn <- function(x, ..., hex = FALSE) {
    replaceSlots(x, table = st_as_binary(x@table, hex = hex), check = FALSE)
}

#' @exportS3Method sf::st_as_text
#' @importFrom sf st_as_text
st_as_text.DuckDBColumn <- function(x, ..., geojson = FALSE) {
    replaceSlots(x, table = st_as_text(x@table, geojson = geojson), check = FALSE)
}

#' @exportS3Method sf::st_boundary
#' @importFrom sf st_boundary
st_boundary.DuckDBColumn <- function(x) {
    replaceSlots(x, table = st_boundary(x@table), check = FALSE)
}

#' @exportS3Method sf::st_centroid
#' @importFrom sf st_centroid
st_centroid.DuckDBColumn <- function(x, ...) {
    replaceSlots(x, table = st_centroid(x@table), check = FALSE)
}

#' @exportS3Method sf::st_convex_hull
#' @importFrom sf st_convex_hull
st_convex_hull.DuckDBColumn <- function(x) {
    replaceSlots(x, table = st_convex_hull(x@table), check = FALSE)
}

#' @exportS3Method sf::st_exterior_ring
#' @importFrom sf st_exterior_ring
st_exterior_ring.DuckDBColumn <- function(x, ...) {
    replaceSlots(x, table = st_exterior_ring(x@table), check = FALSE)
}

#' @exportS3Method sf::st_is_valid
#' @importFrom sf st_is_valid
st_is_valid.DuckDBColumn <- function(x, ...) {
    replaceSlots(x, table = st_is_valid(x@table), check = FALSE)
}

#' @exportS3Method sf::st_line_merge
#' @importFrom sf st_line_merge
st_line_merge.DuckDBColumn <- function(x, ..., directed = FALSE) {
    replaceSlots(x, table = st_line_merge(x@table, directed = directed),
                 check = FALSE)
}

#' @exportS3Method sf::st_make_valid
#' @importFrom sf st_make_valid
st_make_valid.DuckDBColumn <- function(x, ...) {
    replaceSlots(x, table = st_make_valid(x@table), check = FALSE)
}

#' @exportS3Method sf::st_normalize
#' @importFrom sf st_normalize
st_normalize.DuckDBColumn <- function(x, ...) {
    replaceSlots(x, table = st_normalize(x@table), check = FALSE)
}

#' @exportS3Method sf::st_point_on_surface
#' @importFrom sf st_point_on_surface
st_point_on_surface.DuckDBColumn <- function(x) {
    replaceSlots(x, table = st_point_on_surface(x@table), check = FALSE)
}

#' @exportS3Method sf::st_reverse
#' @importFrom sf st_reverse
st_reverse.DuckDBColumn <- function(x) {
    replaceSlots(x, table = st_reverse(x@table), check = FALSE)
}
