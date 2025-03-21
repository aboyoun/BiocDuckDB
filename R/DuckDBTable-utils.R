#' Common operations on DuckDBTable objects
#'
#' @description
#' Common operations on \linkS4class{DuckDBTable} objects.
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
#' In the code snippets below, \code{x} is a DuckDBTable object:
#' \describe{
#'   \item{\code{is.finite(x)}:}{
#'     Returns a DuckDBTable containing logicals that indicate which values are
#'     finite.
#'   }
#'   \item{\code{is.infinite(x)}:}{
#'     Returns a DuckDBTable containing logicals that indicate which values are
#'     infinite.
#'   }
#'   \item{\code{is.nan(x)}:}{
#'     Returns a DuckDBTable containing logicals that indicate which values are
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
#' @section General Methods:
#' In the code snippets below, \code{x} is a DuckDBTable object:
#' \describe{
#'   \item{\code{unique(x)}:}{
#'     Returns a DuckDBTable containing the distinct rows.
#'   }
#'   \item{\code{x \%in\% table}:}{
#'     Returns a DuckDBTable containing logicals that indicate if the
#'     values in each of the columns of \code{x} are in \code{table}.
#'   }
#'   \item{\code{table(...)}:}{
#'     Returns a table containing the counts across the distinct values.
#'   }
#' }
#'
#' @section Sparisty Methods:
#' In the code snippets below, \code{x} is a DuckDBTable object:
#' \describe{
#'   \item{\code{is_nonzero(x)}:}{
#'     Returns a DuckDBTable containing logicals that indicate if the
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
#' @section Spatial Methods:
#' In the code snippets below, \code{x} is a DuckDBTable object:
#' \describe{
#'   \item{\code{st_area(x)}:}{
#'     Returns a DuckDBTable containing the areas of geometries.
#'   }
#'   \item{\code{st_as_binary(x, hex = FALSE)}:}{
#'     Returns the DuckDBTable containing either WKB if \code{hex = FALSE} or
#'     HEXWKB if \code{hex = TRUE} representations of geometries.
#'   }
#'   \item{\code{st_as_text(x, geojson = FALSE)}:}{
#'     Returns the DuckDBTable containing either WKT if \code{geojson = FALSE} or
#'     GeoJSON if \code{geojson = TRUE} representations of geometries.
#'   }
#'   \item{\code{st_boundary(x)}:}{
#'     Returns a DuckDBTable containing the boundaries of geometries.
#'   }
#'   \item{\code{st_centroid(x)}:}{
#'     Returns a DuckDBTable containing the centroids of geometries.
#'   }
#'   \item{\code{st_convex_hull(x)}:}{
#'     Returns a DuckDBTable containing the convex hulls of geometries.
#'   }
#'   \item{\code{st_exterior_ring(x)}:}{
#'     Returns a DuckDBTable containing the exterior rings of geometries.
#'   }
#'   \item{\code{st_is_valid(x)}:}{
#'     Returns a DuckDBTable containing logicals that indicate if the
#'     geometries are valid.
#'   }
#'   \item{\code{st_line_merge(x, directed = FALSE)}:}{
#'     Returns a DuckDBTable containing the merged lines of geometries,
#'     optionally taking direction into account.
#'   }
#'   \item{\code{st_make_valid(x)}:}{
#'     Returns a DuckDBTable containing valid geometries.
#'   }
#'   \item{\code{st_normalize(x)}:}{
#'     Returns a DuckDBTable containing normalized geometries.
#'   }
#'   \item{\code{st_point_on_surface(x)}:}{
#'     Returns a DuckDBTable containing a point on the surface of the input
#'     geometry.
#'   }
#'   \item{\code{st_reverse(x)}:}{
#'     Returns a DuckDBTable containing geometries with the vertice order
#'     reversed.
#'   }
#' }
#'
#' @author Patrick Aboyoun
#'
#' @aliases
#' Ops,DuckDBTable,DuckDBTable-method
#' Ops,DuckDBTable,atomic-method
#' Ops,atomic,DuckDBTable-method
#' Math,DuckDBTable-method
#' Summary,DuckDBTable-method
#'
#' is.finite,DuckDBTable-method
#' is.infinite,DuckDBTable-method
#' is.nan,DuckDBTable-method
#' mean,DuckDBTable-method
#' var,DuckDBTable,ANY-method
#' sd,DuckDBTable-method
#' median.DuckDBTable
#' quantile.DuckDBTable
#' mad,DuckDBTable-method
#' IQR,DuckDBTable-method
#' rowSums,DuckDBTable-method
#' colSums,DuckDBTable-method
#'
#' unique,DuckDBTable-method
#' %in%,DuckDBTable,ANY-method
#' table,DuckDBTable-method
#'
#' is_nonzero,DuckDBTable-method
#' nzcount,DuckDBTable-method
#' is_sparse,DuckDBTable-method
#'
#' st_area.DuckDBTable
#' st_as_binary.DuckDBTable
#' st_as_text.DuckDBTable
#' st_boundary.DuckDBTable
#' st_centroid.DuckDBTable
#' st_convex_hull.DuckDBTable
#' st_exterior_ring.DuckDBTable
#' st_is_valid.DuckDBTable
#' st_line_merge.DuckDBTable
#' st_make_valid.DuckDBTable
#' st_normalize.DuckDBTable
#' st_point_on_surface.DuckDBTable
#' st_reverse.DuckDBTable
#'
#' @include DuckDBTable-class.R
#'
#' @keywords utilities methods
#'
#' @name DuckDBTable-utils
NULL

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Group generic methods
###

#' @importFrom S4Vectors new2
#' @importFrom stats setNames
.Ops.DuckDBTable <- function(.Generic, conn, keycols, fin1, fin2, fout) {
    datacols <- setNames(as.expression(Map(function(x, y) call(.Generic, x, y), fin1, fin2)), fout)
    new2("DuckDBTable", conn = conn, datacols = datacols, keycols = keycols, check = FALSE)
}

#' @export
setMethod("Ops", c(e1 = "DuckDBTable", e2 = "DuckDBTable"), function(e1, e2) {
    if (!isTRUE(all.equal(e1, e2)) || ((ncol(e1) > 1L) && (ncol(e2) > 1L) && (ncol(e1) != ncol(e2)))) {
        stop("can only perform binary operations with compatible objects")
    }
    comb <- cbind(e1, e2)
    fin1 <- head(comb@datacols, ncol(e1))
    fin2 <- tail(comb@datacols, ncol(e2))
    if (ncol(e1) >= ncol(e2)) {
        fout <- colnames(e1)
    } else {
        fout <- colnames(e2)
    }
    .Ops.DuckDBTable(.Generic, conn = comb@conn, keycols = comb@keycols, fin1 = fin1, fin2 = fin2, fout = fout)
})

#' @export
setMethod("Ops", c(e1 = "DuckDBTable", e2 = "atomic"), function(e1, e2) {
    if (length(e2) != 1L) {
        stop("can only perform binary operations with a scalar value")
    }
    .Ops.DuckDBTable(.Generic, conn = e1@conn, keycols = e1@keycols, fin1 = e1@datacols, fin2 = e2, fout = colnames(e1))
})

#' @export
setMethod("Ops", c(e1 = "atomic", e2 = "DuckDBTable"), function(e1, e2) {
    if (length(e1) != 1L) {
        stop("can only perform binary operations with a scalar value")
    }
    .Ops.DuckDBTable(.Generic, conn = e2@conn, keycols = e2@keycols, fin1 = e1, fin2 = e2@datacols, fout = colnames(e2))
})

#' @export
#' @importFrom S4Vectors endoapply
setMethod("Math", "DuckDBTable", function(x) {
    datacols <-
      switch(.Generic,
             abs =,
             sign =,
             sqrt =,
             ceiling =,
             floor =,
             trunc =,
             log =,
             log10 =,
             log2 =,
             acos =,
             acosh =,
             asin =,
             asinh =,
             atan =,
             atanh =,
             exp =,
             cos =,
             cosh =,
             sin =,
             sinh =,
             tan =,
             tanh =,
             gamma =,
             lgamma = {
                endoapply(x@datacols, function(j) call(.Generic, j))
             },
             stop("unsupported Math operator: ", .Generic))
    replaceSlots(x, datacols = datacols, check = FALSE)
})

#' @importFrom dplyr pull summarize
.pull.aggregagte <- function(x, fun, na.rm = FALSE) {
    if (length(x@datacols) != 1L) {
        stop("aggregation requires a single datacols")
    }
    if (na.rm) {
        aggr <- call(fun, x@datacols[[1L]], na.rm = TRUE)
    } else {
        aggr <- call(fun, x@datacols[[1L]])
    }
    pull(summarize(tblconn(x), !!aggr))
}

#' @export
setMethod("Summary", "DuckDBTable", function(x, ..., na.rm = FALSE) {
    if (.Generic == "range") {
        if (length(x@datacols) != 1L) {
            stop("aggregation requires a single datacols")
        }
        aggr <- list(min = call("min", x@datacols[[1L]], na.rm = TRUE),
                     max = call("max", x@datacols[[1L]], na.rm = TRUE))
        unlist(as.data.frame(summarize(tblconn(x), !!!aggr)), use.names = FALSE)
    } else if (.Generic == "sum") {
        .pull.aggregagte(x, "fsum")
    } else {
        .pull.aggregagte(x, .Generic, na.rm = TRUE)
    }
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Numerical methods
###

#' @export
#' @importFrom S4Vectors endoapply
setMethod("is.finite", "DuckDBTable", function(x) {
    datacols <- endoapply(x@datacols, function(j) call("isfinite", j))
    replaceSlots(x, datacols = datacols, check = FALSE)
})

#' @export
#' @importFrom S4Vectors endoapply
setMethod("is.infinite", "DuckDBTable", function(x) {
    datacols <- endoapply(x@datacols, function(j) call("isinf", j))
    replaceSlots(x, datacols = datacols, check = FALSE)
})

#' @export
#' @importFrom S4Vectors endoapply
setMethod("is.nan", "DuckDBTable", function(x) {
    datacols <- endoapply(x@datacols, function(j) call("isnan", j))
    replaceSlots(x, datacols = datacols, check = FALSE)
})

#' @export
#' @importFrom BiocGenerics mean
setMethod("mean", "DuckDBTable", function(x, ...) {
    .pull.aggregagte(x, "mean", na.rm = TRUE)
})

#' @export
#' @importFrom BiocGenerics var
setMethod("var", "DuckDBTable", function(x, y = NULL, na.rm = FALSE, use)  {
    if (!is.null(y)) {
        stop("covariance is not supported")
    }
    .pull.aggregagte(x, "var", na.rm = TRUE)
})

#' @export
#' @importFrom BiocGenerics sd
setMethod("sd", "DuckDBTable", function(x, na.rm = FALSE) {
    .pull.aggregagte(x, "sd", na.rm = TRUE)
})

#' @exportS3Method stats::median
#' @importFrom stats median
median.DuckDBTable <- function(x, na.rm = FALSE, ...) {
    .pull.aggregagte(x, "median", na.rm = TRUE)
}

#' @exportS3Method stats::quantile
#' @importFrom dplyr summarize
#' @importFrom S4Vectors isSingleNumber
#' @importFrom stats quantile
quantile.DuckDBTable <-
function(x, probs = seq(0, 1, 0.25), na.rm = FALSE, names = TRUE, type = 7, digits = 7, ...) {
    if (length(x@datacols) != 1L) {
        stop("aggregation requires a single datacols")
    }
    if (!isSingleNumber(type) || !(type %in% c(1L, 7L))) {
        stop("'type' must be 1 or 7")
    } else if (type == 1L) {
        fun <- "quantile_disc"
    } else {
        fun <- "quantile_cont"
    }
    aggr <- lapply(probs, function(p) call(fun, x@datacols[[1L]], p))
    ans <- unlist(as.data.frame(summarize(tblconn(x), !!!aggr)), use.names = FALSE)
    if (names) {
        stopifnot(isSingleNumber(digits), digits >= 1)
        names(ans) <- paste0(formatC(100 * probs, format = "fg", width = 1, digits = digits), "%")
    }
    ans
}

#' @export
#' @importFrom BiocGenerics mad
setMethod("mad", "DuckDBTable",
function(x, center = median(x), constant = 1.4826, na.rm = FALSE, low = FALSE, high = FALSE) {
    constant * .pull.aggregagte(x, "mad")
})

#' @export
#' @importFrom BiocGenerics IQR
setMethod("IQR", "DuckDBTable", function(x, na.rm = FALSE, type = 7) {
    diff(quantile(x, c(0.25, 0.75), na.rm = na.rm, names = FALSE, type = type))
})

#' @export
#' @importFrom dplyr group_by summarize
#' @importFrom DelayedArray rowSums
#' @importFrom S4Vectors isSingleNumber new2
setMethod("rowSums", "DuckDBTable", function(x, na.rm = FALSE, dims = 1, ...) {
    if (nkey(x) < 2L) {
        stop("'x' must be an array of at least two dimensions")
    }
    if (!isSingleNumber(dims) || dims < 1L || dims >= nkey(x)) {
        stop("invalid 'dims'")
    }
    if (length(x@datacols) != 1L) {
        stop("requires a single datacols")
    }
    datacols <- x@datacols
    keycols <- head(x@keycols, dims)
    groups <- lapply(names(keycols), as.name)
    aggr <- sapply(datacols, function(y) call("sum", y, na.rm = TRUE), simplify = FALSE)
    conn <- summarize(group_by(tblconn(x, filter = FALSE), !!!groups), !!!aggr)
    new2("DuckDBTable", conn = conn, datacols = datacols, keycols = keycols, check = FALSE)
})

#' @export
#' @importFrom dplyr group_by summarize
#' @importFrom DelayedArray colSums
#' @importFrom S4Vectors isSingleNumber new2
setMethod("colSums", "DuckDBTable", function(x, na.rm = FALSE, dims = 1, ...) {
    nk <- nkey(x)
    if (nk < 2L) {
        stop("'x' must be an array of at least two dimensions")
    }
    if (!isSingleNumber(dims) || dims < 1L || dims >= nk) {
        stop("invalid 'dims'")
    }
    if (length(x@datacols) != 1L) {
        stop("requires a single datacols")
    }
    datacols <- x@datacols
    keycols <- tail(x@keycols, nk - dims)
    groups <- lapply(names(keycols), as.name)
    aggr <- sapply(datacols, function(y) call("sum", y, na.rm = TRUE), simplify = FALSE)
    conn <- summarize(group_by(tblconn(x, filter = FALSE), !!!groups), !!!aggr)
    new2("DuckDBTable", conn = conn, datacols = datacols, keycols = keycols, check = FALSE)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Set methods
###

#' @export
#' @importFrom BiocGenerics unique
#' @importFrom dplyr distinct mutate
setMethod("unique", "DuckDBTable",
function (x, incomparables = FALSE, fromLast = FALSE, ...)  {
    if (!isFALSE(incomparables)) {
        .NotYetUsed("incomparables != FALSE")
    }
    conn <- tblconn(x, filter = FALSE)
    datacols <- x@datacols
    keycols <- tail(make.unique(c(colnames(conn), "row_number"), sep = "_"), 1L)
    keycols <- setNames(list(call("row_number")), keycols)
    conn <- distinct(conn, !!!as.list(datacols))
    conn <- mutate(conn, !!!keycols)
    keycols[[1L]] <- .keycols.row_number(conn)
    replaceSlots(x, conn = conn, keycols = keycols, check = FALSE)
})

#' @export
#' @importFrom BiocGenerics %in%
#' @importFrom S4Vectors endoapply
setMethod("%in%", c(x = "DuckDBTable", table = "ANY"), function(x, table) {
    datacols <- endoapply(x@datacols, function(j) call("%in%", j, table))
    replaceSlots(x, datacols = datacols, check = FALSE)
})


#' @export
#' @importFrom BiocGenerics table
#' @importFrom dplyr group_by n summarize
#' @importFrom stats setNames
setMethod("table", "DuckDBTable", function(...) {
    args <- list(...)
    if (length(args) != 1L) {
        stop("\"table\" method for DuckDB data can only take one input object")
    }
    x <- args[[1L]]
    conn <- tblconn(x)
    groups <- as.list(x@datacols)
    counts <- as.data.frame(summarize(group_by(conn, !!!groups), count = n(), .groups = "drop"))
    dnames <- lapply(counts[seq_along(groups)], function(j) as.character(sort(unique(j))))
    ans <- array(0L, dim = lengths(dnames, use.names = FALSE), dimnames = dnames)
    ans[do.call(cbind, lapply(counts[seq_along(groups)], as.character))] <- as.integer(counts[["count"]])
    class(ans) <- "table"
    ans
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Sparsity methods
###

#' @importFrom bit64 as.integer64
.zeros <- list("logical" = FALSE,
               "integer" = 0L,
               "integer64" = as.integer64(0L),
               "double" = 0,
               "character" = "",
               "raw" = "")

#' @export
#' @importFrom SparseArray is_nonzero
setMethod("is_nonzero", "DuckDBTable", function(x) {
    datacols <- x@datacols
    ctypes <- coltypes(x)
    for (j in names(ctypes)) {
        datacols[[j]] <- switch(ctypes[j],
                                logical = datacols[[j]],
                                integer =,
                                integer64 =,
                                double =,
                                character =,
                                raw = call("!=", datacols[[j]], .zeros[[ctypes[j]]]),
                                TRUE)
    }
    replaceSlots(x, datacols = datacols, check = FALSE)
})

#' @export
#' @importFrom SparseArray nzcount
#' @importFrom stats setNames
setMethod("nzcount", "DuckDBTable", function(x) {
    tbl <- is_nonzero(x)
    coltypes(tbl) <- rep.int("integer", ncol(tbl))
    datacols <- setNames(as.expression(Reduce(function(x, y) call("+", x, y), tbl@datacols)), "nonzero")
    tbl <- replaceSlots(tbl, datacols = datacols, check = FALSE)
    cnt <- sum(tbl)
    if (is.na(cnt)) {
        cnt <- 0L
    }
    cnt
})

#' @export
#' @importFrom S4Arrays is_sparse
setMethod("is_sparse", "DuckDBTable", function(x) {
    TRUE
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Spatial methods
###

#' @exportS3Method sf::st_area
#' @importFrom sf st_area
st_area.DuckDBTable <- function(x, ...) {
    datacols <- endoapply(x@datacols, function(j) call("ST_Area", j))
    replaceSlots(x, datacols = datacols, check = FALSE)
}

#' @exportS3Method sf::st_as_binary
#' @importFrom sf st_as_binary
st_as_binary.DuckDBTable <- function(x, ..., hex = FALSE) {
    fun <- if (isTRUE(hex)) "ST_AsHEXWKB" else "ST_AsWKB"
    datacols <- endoapply(x@datacols, function(j) call(fun, j))
    replaceSlots(x, datacols = datacols, check = FALSE)
}

#' @exportS3Method sf::st_as_text
#' @importFrom sf st_as_text
st_as_text.DuckDBTable <- function(x, ..., geojson = FALSE) {
    fun <- if (isTRUE(geojson)) "ST_AsGeoJSON" else "ST_AsText"
    datacols <- endoapply(x@datacols, function(j) call(fun, j))
    replaceSlots(x, datacols = datacols, check = FALSE)
}

#' @exportS3Method sf::st_boundary
#' @importFrom sf st_boundary
st_boundary.DuckDBTable <- function(x) {
    datacols <- endoapply(x@datacols, function(j) call("ST_Boundary", j))
    replaceSlots(x, datacols = datacols, check = FALSE)
}

#' @exportS3Method sf::st_centroid
#' @importFrom sf st_centroid
st_centroid.DuckDBTable <- function(x, ...) {
    datacols <- endoapply(x@datacols, function(j) call("ST_Centroid", j))
    replaceSlots(x, datacols = datacols, check = FALSE)
}

#' @exportS3Method sf::st_convex_hull
#' @importFrom sf st_convex_hull
st_convex_hull.DuckDBTable <- function(x) {
    datacols <- endoapply(x@datacols, function(j) call("ST_ConvexHull", j))
    replaceSlots(x, datacols = datacols, check = FALSE)
}

#' @exportS3Method sf::st_exterior_ring
#' @importFrom sf st_exterior_ring
st_exterior_ring.DuckDBTable <- function(x, ...) {
    datacols <- endoapply(x@datacols, function(j) call("ST_ExteriorRing", j))
    replaceSlots(x, datacols = datacols, check = FALSE)
}

#' @exportS3Method sf::st_is_valid
#' @importFrom sf st_is_valid
st_is_valid.DuckDBTable <- function(x, ...) {
    datacols <- endoapply(x@datacols, function(j) call("ST_IsValid", j))
    replaceSlots(x, datacols = datacols, check = FALSE)
}

#' @exportS3Method sf::st_line_merge
#' @importFrom sf st_line_merge
st_line_merge.DuckDBTable <- function(x, ..., directed = FALSE) {
    datacols <- endoapply(x@datacols, function(j) call("ST_LineMerge", j, directed))
    replaceSlots(x, datacols = datacols, check = FALSE)
}

#' @exportS3Method sf::st_make_valid
#' @importFrom sf st_make_valid
st_make_valid.DuckDBTable <- function(x, ...) {
    datacols <- endoapply(x@datacols, function(j) call("ST_MakeValid", j))
    replaceSlots(x, datacols = datacols, check = FALSE)
}

#' @exportS3Method sf::st_normalize
#' @importFrom sf st_normalize
st_normalize.DuckDBTable <- function(x, ...) {
    datacols <- endoapply(x@datacols, function(j) call("ST_Normalize", j))
    replaceSlots(x, datacols = datacols, check = FALSE)
}

#' @exportS3Method sf::st_point_on_surface
#' @importFrom sf st_point_on_surface
st_point_on_surface.DuckDBTable <- function(x) {
    datacols <- endoapply(x@datacols, function(j) call("ST_PointOnSurface", j))
    replaceSlots(x, datacols = datacols, check = FALSE)
}

#' @exportS3Method sf::st_reverse
#' @importFrom sf st_reverse
st_reverse.DuckDBTable <- function(x) {
    datacols <- endoapply(x@datacols, function(j) call("ST_Reverse", j))
    replaceSlots(x, datacols = datacols, check = FALSE)
}
