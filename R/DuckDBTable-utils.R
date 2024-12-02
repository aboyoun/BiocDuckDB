#' DuckDBTable Utilities
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
#' @include DuckDBTable-class.R
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
    pull(summarize(x@conn, !!aggr))
}

#' @export
setMethod("Summary", "DuckDBTable", function(x, ..., na.rm = FALSE) {
    if (.Generic == "range") {
        if (length(x@datacols) != 1L) {
            stop("aggregation requires a single datacols")
        }
        aggr <- list(min = call("min", x@datacols[[1L]], na.rm = TRUE),
                     max = call("max", x@datacols[[1L]], na.rm = TRUE))
        unlist(as.data.frame(summarize(x@conn, !!!aggr)), use.names = FALSE)
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
    ans <- unlist(as.data.frame(summarize(x@conn, !!!aggr)), use.names = FALSE)
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
    conn <- summarize(group_by(x@conn, !!!groups), !!!aggr)
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
    conn <- summarize(group_by(x@conn, !!!groups), !!!aggr)
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
    conn <- x@conn
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
    conn <- x@conn
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
    sum(tbl)
})

#' @export
#' @importFrom S4Arrays is_sparse
setMethod("is_sparse", "DuckDBTable", function(x) {
    (ncol(x) == 1L) && ((nzcount(x) / nrow(x)) < 0.5)
})
