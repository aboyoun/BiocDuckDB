#' DuckDBTable objects
#'
#' @description
#' DuckDBTable is a low-level helper class for representing a
#' pointer to a \code{tbl_duckdb_connection} object.
#'
#' @param conn Either a character vector containing the paths to parquet, csv,
#' or gzipped csv data files; a string that defines a duckdb \code{read_*} data
#' source; a \code{DuckDBDataFrame} object; or a \code{tbl_duckdb_connection}
#' object.
#' @param datacols Either a character vector of column names from \code{conn}
#' or a named \code{expression} that will be evaluated in the context of `conn`
#' that defines the data.
#' @param keycols An optional character vector of column names from \code{conn}
#' that will define the primary key, or a named list of character vectors where
#' the names of the list define the key and the character vectors set the
#' distinct values for the key. If missing, a \code{row_number} column is
#' created as an identifier.
#' @param type An optional named character vector where the names specify the
#' column names and the values specify the column type; one of
#' \code{"logical"}, \code{"integer"}, \code{"integer64"}, \code{"double"}, or
#' \code{"character"}.
#'
#' @author Patrick Aboyoun
#'
#' @examples
#' # Create a data.frame from the Titanic data
#' df <- do.call(expand.grid, c(dimnames(Titanic), stringsAsFactors = FALSE))
#' df$fate <- as.integer(Titanic[as.matrix(df)])
#'
#' # Write data to a parquet file
#' tf <- tempfile(fileext = ".parquet")
#' on.exit(unlink(tf))
#' arrow::write_parquet(df, tf)
#'
#' tbl <- DuckDBTable(tf, datacols = "fate", keycols = c("Class", "Sex", "Age", "Survived"))
#'
#' @aliases
#' DuckDBTable-class
#' show,DuckDBTable-method
#' [,DuckDBTable,ANY,ANY,ANY-method
#' all.equal.DuckDBTable
#' as.data.frame,DuckDBTable-method
#' bindCOLS,DuckDBTable-method
#' colnames,DuckDBTable-method
#' colnames<-,DuckDBTable-method
#' coltypes
#' coltypes,DuckDBTable-method
#' coltypes<-
#' coltypes<-,DuckDBTable-method
#' dbconn,DuckDBTable-method
#' is_nonzero,DuckDBTable-method
#' is_sparse,DuckDBTable-method
#' is.finite,DuckDBTable-method
#' is.infinite,DuckDBTable-method
#' is.nan,DuckDBTable-method
#' ncol,DuckDBTable-method
#' nrow,DuckDBTable-method
#' nzcount,DuckDBTable-method
#' rownames,DuckDBTable-method
#' tblconn,DuckDBTable-method
#' unique,DuckDBTable-method
#' Ops,DuckDBTable,DuckDBTable-method
#' Ops,DuckDBTable,atomic-method
#' Ops,atomic,DuckDBTable-method
#' Math,DuckDBTable-method
#' Summary,DuckDBTable-method
#' mean,DuckDBTable-method
#' median.DuckDBTable
#' quantile.DuckDBTable
#' var,DuckDBTable,ANY-method
#' sd,DuckDBTable-method
#' mad,DuckDBTable-method
#' IQR,DuckDBTable-method
#'
#' @include DuckDBConnection.R
#' @include keynames.R
#' @include tblconn.R
#'
#' @name DuckDBTable
NULL

setOldClass("tbl_duckdb_connection")

replaceSlots <- BiocGenerics:::replaceSlots

#' @importFrom bit64 NA_integer64_
#' @importFrom dplyr n pull summarize
.keycols.row_number <- function(conn) {
    c(NA_integer64_, - pull(summarize(conn, n = n())))
}

#' @export
#' @importClassesFrom BiocGenerics OutOfMemoryObject
#' @importClassesFrom S4Vectors RectangularData
#' @importFrom stats setNames
setClass("DuckDBTable", contains = c("RectangularData", "OutOfMemoryObject"),
    slots = c(conn = "tbl_duckdb_connection", datacols = "expression", keycols = "list"),
    prototype = prototype(conn = structure(list(),
                                           class = c("tbl_duckdb_connection", "tbl_dbi",
                                                     "tbl_sql", "tbl_lazy", "tbl")),
                          datacols = setNames(expression(), character()),
                          keycols = setNames(list(), character())))

#' @importFrom S4Vectors setValidity2
setValidity2("DuckDBTable", function(x) {
    msg <- NULL
    if (is.null(names(x@keycols))) {
        msg <- c(msg, "'keycols' slot must be a named list")
    }
    if (!all(names(x@keycols) %in% colnames(x@conn))) {
        msg <- c(msg, "all names in 'keycols' slot must match column names in 'conn'")
    }
    for (i in seq_along(x@keycols)) {
        if (!is.atomic(x@keycols[[i]])) {
            msg <- c(msg, "all elements in 'keycols' slot must be atomic")
            break
        }
    }
    if (is.null(names(x@datacols))) {
        msg <- c(msg, "'datacols' slot must be a named expression")
    }
    if (length(intersect(names(x@keycols), names(x@datacols)))) {
        msg <- c(msg, "names in 'keycols' and 'datacols' slots must be unique")
    }
    if (is.null(msg)) {
        TRUE
    } else {
        msg
    }
})

#' @export
#' @importFrom S4Vectors classNameForDisplay
setMethod("show", "DuckDBTable", function(object) {
    if (nkey(object) == 0L) {
        cat(sprintf("%s object\n", classNameForDisplay(object)))
    } else {
        cat(sprintf("%s object with key (%s)\n",
                    classNameForDisplay(object),
                    paste(keynames(object), collapse = ", ")))
    }
    if (length(object@conn) > 0L) {
        print(object@conn)
        expr <- deparse(object@datacols,
                        width.cutoff = getOption("width", 60L) - 6L)
        expr <- sub("^[ \t\r\n]+", "      ", sub("\\)", "",
                    sub("^expression\\(", "", expr)))
        cat(sprintf("cols: %s\n", paste(expr, collapse = "\n")))
    }
    invisible(NULL)
})

#' @export
#' @importFrom BiocGenerics dbconn
setMethod("dbconn", "DuckDBTable", function(x) x@conn$src$con)

#' @export
setMethod("tblconn", "DuckDBTable", function(x) x@conn)

setGeneric(".has_row_number", function(x) standardGeneric(".has_row_number"))

#' @importFrom bit64 as.integer64 is.integer64
setMethod(".has_row_number", "DuckDBTable", function(x) {
    if (length(x@keycols) == 1L) {
        key1 <- x@keycols[[1L]]
        is.integer64(key1) && (length(key1) == 2L) && is.na(key1[1L]) && (key1[2L] <= as.integer64(0L))
    } else {
        FALSE
    }
})

#' @export
setMethod("nkey", "DuckDBTable", function(x) {
    if (.has_row_number(x)) 0L else length(x@keycols)
})

#' @export
setMethod("nkeydim", "DuckDBTable", function(x) {
    if (length(x@conn) == 0L) {
        0L
    } else if (.has_row_number(x)) {
        abs(x@keycols[[1L]][2L])
    } else {
        lengths(x@keycols, use.names = FALSE)
    }
})

#' @export
#' @importFrom BiocGenerics nrow
#' @importFrom bit64 as.integer64
setMethod("nrow", "DuckDBTable", function(x) {
    nr <- prod(as.integer64(nkeydim(x)))
    if (nr <= as.integer64(.Machine$integer.max)) {
        as.integer(nr)
    } else {
        nr
    }
})

#' @export
#' @importFrom BiocGenerics ncol
setMethod("ncol", "DuckDBTable", function(x) length(x@datacols))

#' @export
setMethod("keynames", "DuckDBTable", function(x) {
    if (.has_row_number(x)) character(0L) else names(x@keycols)
})

#' @export
#' @importFrom dplyr pull select
setMethod("keydimnames", "DuckDBTable", function(x) {
    if (.has_row_number(x)) {
        list(as.character(pull(select(x@conn, !!as.name(names(x@keycols))))))
    } else {
        lapply(x@keycols, function(y) names(y) %||% as.character(y))
    }
})

#' @export
setReplaceMethod("keydimnames", "DuckDBTable", function(x, value) {
    if (!is.list(value)) {
        stop("'value' must be a list of vectors")
    }
    keycols <- x@keycols
    if (is.null(names(value))) {
        if (length(value) != length(keycols)) {
            stop("if 'value' is unnamed, then it must match length of 'keycols'")
        }
        names(value) <- names(keycols)
    }
    for (i in names(value)) {
        names(keycols[[i]]) <- value[[i]]
    }
    replaceSlots(x, keycols = keycols, check = FALSE)
})

#' @export
#' @importFrom BiocGenerics rownames
setMethod("rownames", "DuckDBTable", function(x, do.NULL = TRUE, prefix = "row") {
    if (length(x@conn) == 0L) {
        NULL
    } else if (length(x@keycols) == 1L) {
        keydimnames(x)[[1L]]
    } else {
        stop("rownames is not supported for multi-dimensional keys")
    }
})

#' @export
#' @importFrom BiocGenerics colnames
setMethod("colnames", "DuckDBTable", function(x, do.NULL = TRUE, prefix = "col") {
    names(x@datacols)
})

#' @export
#' @importFrom BiocGenerics colnames<-
setReplaceMethod("colnames", "DuckDBTable", function(x, value) {
    datacols <- x@datacols
    names(datacols) <- value
    replaceSlots(x, datacols = datacols, check = FALSE)
})

#' @importFrom bit64 is.integer64
.get_type <- function(column) {
    if (is.integer64(column)) {
        "integer64"
    } else if (inherits(column, "Date")) {
        "Date"
    } else if (inherits(column, "POSIXct")) {
        "POSIXct"
    } else if (is.list(column)) {
        "raw"
    } else {
        DelayedArray::type(column)
    }
}

.cast_cols <- function(datacols, value) {
    if (is.null(names(value)) && (length(value) == length(datacols))) {
        names(value) <- names(datacols)
    }
    for (j in names(value)) {
        cast <- switch(value[j],
                       logical = "as.logical",
                       integer = "as.integer",
                       integer64 = "as.integer64",
                       double = "as.double",
                       character = "as.character",
                       stop("'type' must be one of 'logical', 'integer', 'integer64', 'double', or 'character'"))
        datacols[[j]] <- call(cast, datacols[[j]])
    }
    datacols
}

#' @export
setGeneric("coltypes", function(x) standardGeneric("coltypes"))

#' @export
setMethod("coltypes", "DuckDBTable", function(x) {
    i <- sapply(names(x@keycols), function(x) integer(), simplify = FALSE)
    empty <- .subset_DuckDBTable(x, i, drop = FALSE)
    vapply(as.data.frame(empty)[names(x@datacols)], .get_type, character(1L))
})

#' @export
setGeneric("coltypes<-", function(x, value) standardGeneric("coltypes<-"))

#' @export
setReplaceMethod("coltypes", "DuckDBTable", function(x, value) {
    datacols <- .cast_cols(x@datacols, value)
    replaceSlots(x, datacols = datacols, check = FALSE)
})

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

#' @importFrom bit64 as.integer64
#' @importFrom dplyr distinct filter pull select
.subset_DuckDBTable <- function(x, i, j, ..., drop = TRUE) {
    conn <- x@conn
    datacols <- x@datacols
    if (!missing(j)) {
        datacols <- datacols[j]
    }

    keycols <- x@keycols
    if (!missing(i)) {
        if (!is.list(i) || is.null(names(i))) {
            stop("'i' must be a named list")
        }
        for (k in intersect(names(keycols), names(i))) {
            sub <- i[[k]]
            if (is.atomic(sub)) {
                if (.has_row_number(x)) {
                    if (is.numeric(sub)) {
                        keep <- call("%in%", as.name(k), as.integer64(sub))
                        conn <- filter(conn, !!keep)
                        keycols[[1L]] <- .keycols.row_number(conn)
                    } else {
                        stop("unsupported 'i' for row subsetting with row_number")
                    }
                } else if (is.character(sub) && is.null(names(keycols[[k]]))) {
                    keycols[[k]] <- sub
                } else {
                    keycols[[k]] <- keycols[[k]][sub]
                }
            } else if (is(sub, "DuckDBColumn") &&
                       is.logical(as.vector(head(sub, 0L))) &&
                       isTRUE(all.equal(as(x, "DuckDBTable"), sub@table))) {
                keep <- sub@table@datacols[[1L]]
                conn <- filter(conn, !!keep)
                if (.has_row_number(x)) {
                    keycols[[1L]] <- .keycols.row_number(conn)
                } else {
                    for (kname in names(keycols)) {
                        kdnames <- pull(distinct(select(conn, !!as.name(kname))))
                        keycols[[kname]] <- keycols[[kname]][match(kdnames, keycols[[kname]])]
                    }
                }
            } else {
                stop("unsupported 'i' for row subsetting")
            }
        }
    }

    replaceSlots(x, conn = conn, datacols = datacols, keycols = keycols, ..., check = FALSE)
}

#' @export
setMethod("[", "DuckDBTable", .subset_DuckDBTable)

#' @export
#' @importFrom S4Vectors bindCOLS
setMethod("bindCOLS", "DuckDBTable",
function(x, objects = list(), use.names = TRUE, ignore.mcols = FALSE, check = TRUE) {
    datacols <- x@datacols

    for (i in seq_along(objects)) {
        obj <- objects[[i]]
        if (!is(obj, "DuckDBTable")) {
            stop("all objects must be of class 'DuckDBTable'")
        }
        if (!isTRUE(all.equal(x, obj))) {
            stop("all objects must share a compatible 'DuckDBTable' structure")
        }
        newname <- names(objects)[i]
        if (!is.null(newname) && nzchar(newname)) {
            if (ncol(obj) > 1L) {
                colnames(obj) <- paste(newname, colnames(obj), sep = "_")
            }
            colnames(obj) <- newname
        }
        datacols <- c(datacols, obj@datacols)
    }
    names(datacols) <- make.unique(names(datacols), sep = "_")
    replaceSlots(x, datacols = datacols, check = FALSE)
})

#' @exportS3Method base::all.equal
all.equal.DuckDBTable <- function(target, current, check.datacols = FALSE, ...) {
    if (!is(current, "DuckDBTable")) {
        return("current is not a DuckDBTable")
    }
    target <- as(target, "DuckDBTable")
    current <- as(current, "DuckDBTable")
    if (!check.datacols) {
        target <- target[, integer()]
        current <- current[, integer()]
    }
    callGeneric(unclass(target), unclass(current), ...)
}

#' @importFrom S4Vectors new2
#' @importFrom stats setNames
.Ops.DuckDBTable <- function(.Generic, conn, keycols, fin1, fin2, fout) {
    datacols <- setNames(as.expression(Map(function(x, y) call(.Generic, x, y), fin1, fin2)), fout)
    new2("DuckDBTable", conn = conn, datacols = datacols, keycols = keycols, check = FALSE)
}

#' @export
setMethod("Ops", c(e1 = "DuckDBTable", e2 = "DuckDBTable"), function(e1, e2) {
    if (!isTRUE(all.equal(e1, e2)) || ((ncol(e1) > 1L) && (ncol(e2) > 1L) && (ncol(e1) != ncol(e2)))) {
        stop("can only perform arithmetic operations with compatible objects")
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

#' @export
#' @importFrom BiocGenerics mean
setMethod("mean", "DuckDBTable", function(x, ...) {
    .pull.aggregagte(x, "mean", na.rm = TRUE)
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
#' @importFrom BiocGenerics as.data.frame
#' @importFrom dplyr filter mutate select
setMethod("as.data.frame", "DuckDBTable",
function(x, row.names = NULL, optional = FALSE, ...) {
    conn <- x@conn
    keycols <- x@keycols
    datacols <- as.list(x@datacols)

    if (!.has_row_number(x)) {
        for (i in names(keycols)) {
            set <- keycols[[i]]
            conn <- filter(conn, !!as.name(i) %in% set)
        }
    }

    conn <- mutate(conn, !!!as.list(datacols))
    conn <- select(conn, c(names(keycols), names(datacols)))

    # Allow for 1 extra row to check for duplicate keys
    length <- nrow(x) + 1L
    conn <- head(conn, n = length)

    df <- as.data.frame(conn)[, c(names(keycols), names(datacols))]
    if (anyDuplicated(df[, names(keycols)])) {
        stop("duplicate keys found in the DuckDB table")
    }

    # Coerce list of raws to character
    tochar <- function(col) tryCatch(rawToChar(col), error = function(e) "")
    for (j in seq_along(df)) {
        if (is.list(df[[j]])) {
            df[[j]] <- sapply(df[[j]], tochar, USE.NAMES = FALSE)
        }
    }

    df
})

.wrapFile <- function(x, read) {
    x <- sprintf("'%s'", x)
    if (length(x) > 1L) {
        x <- sprintf("[%s]", paste(x, collapse = ", "))
    }
    sprintf("%s(%s)", read, x)
}

#' @importFrom S4Vectors isSingleString
.wrapConn <- function(x) {
    if (all(grepl("(?i)\\.(csv|tsv)(\\.gz)?$", x))) {
        x <- .wrapFile(x, "read_csv")
    } else if (all(grepl("(?i)\\.(parquet|pq)$", x))) {
        x <- .wrapFile(x, "read_parquet")
    } else if (isSingleString(x) && dir.exists(x)) {
            files <- list.files(x, recursive = TRUE)
            if (any(all(grepl("(?i)\\.(parquet|pq)$", files)))) {
                x <- .wrapFile(file.path(x, "**"), "read_parquet")
            }
        }
    x
}

#' @export
#' @importFrom dplyr distinct mutate pull select tbl
#' @importFrom S4Vectors new2
#' @importFrom stats setNames
#' @rdname DuckDBTable
DuckDBTable <-
function(conn, datacols = colnames(conn), keycols = NULL, type = NULL) {
    # Acquire the connection if it is a string
    if (is.character(conn)) {
        conn <- tbl(acquireDuckDBConn(), .wrapConn(conn))
    } else if (is(conn, "DuckDBTable")) {
        conn <- conn@conn
    } else if (!inherits(conn, "tbl_duckdb_connection")) {
        stop("'conn' must be a 'tbl_duckdb_connection' object")
    }

    # Ensure 'datacols' is a named expression
    if (length(datacols) == 0L) {
        datacols <- setNames(expression(), character())
    } else {
        if (is.character(datacols)) {
            datacols <- sapply(datacols, as.name, simplify = FALSE)
            if (!is.null(type)) {
                if (is.null(names(type)) || length(setdiff(names(type), names(datacols)))) {
                    stop("all names in 'type' must have a corresponding name in 'datacols'")
                }
                datacols <- .cast_cols(datacols, type)
            }
        }
        datacols <- as.expression(datacols)
        if (is.null(names(datacols))) {
            stop("'datacols' must be a named expression")
        }
    }

    # Ensure 'keycols' is a named list of vectors
    if (is.null(keycols)) {
        keycols <- tail(make.unique(c(colnames(conn), "row_number"), sep = "_"), 1L)
        keycols <- setNames(list(call("row_number")), keycols)
        conn <- mutate(conn, !!!keycols)
        keycols[[1L]] <- .keycols.row_number(conn)
    } else if (is.character(keycols)) {
        keycols <- sapply(keycols, function(x) NULL, simplify = FALSE)
    }
    if (!is.list(keycols) || is.null(names(keycols))) {
        stop("'keycols' must be a character vector or a named list of vectors")
    }
    for (k in names(keycols)) {
        if (is.null(keycols[[k]])) {
            keycols[[k]] <- pull(distinct(select(conn, !!as.name(k))))
        }
    }

    new2("DuckDBTable", conn = conn, datacols = datacols, keycols = keycols, check = FALSE)
}
