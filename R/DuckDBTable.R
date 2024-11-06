#' DuckDBTable objects
#'
#' @description
#' DuckDBTable is a low-level helper class for representing a
#' pointer to a \code{tbl_duckdb_connection} object.
#'
#' @param conn Either a string containing the path to the data files or a
#' \code{tbl_duckdb_connection} object.
#' @param key Either a character vector or a list of character vectors
#' containing the names of the columns that comprise the primary key.
#' @param fact Either a character vector containing the names of the columns
#' in the Parquet data that specify the facts
#' @param type An optional named character vector where the names specify the
#' column names and the values specify the column type; one of
#' \code{"logical"}, \code{"integer"}, \code{"double"}, or \code{"character"}.
#' @param ... Further arguments to be passed to \code{read_parquet}.
#'
#' @author Patrick Aboyoun
#'
#' @examples
#' # Create a data.frame from the Titanic data
#' df <- do.call(expand.grid, c(dimnames(Titanic), stringsAsFactors = FALSE))
#' df$fate <- as.integer(Titanic[as.matrix(df)])
#'
#' # Write data to a parquet file
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' arrow::write_parquet(df, tf)
#'
#' tbl <- DuckDBTable(tf, key = c("Class", "Sex", "Age", "Survived"), fact = "fate")
#'
#' @aliases
#' DuckDBTable-class
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
#' ncol,DuckDBTable-method
#' nrow,DuckDBTable-method
#' nzcount,DuckDBTable-method
#' rownames,DuckDBTable-method
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
#'
#' @include LanguageList.R
#' @include acquireTable.R
#' @include keynames.R
#'
#' @name DuckDBTable
NULL

setOldClass("tbl_duckdb_connection")

#' @importFrom S4Vectors isTRUEorFALSE
initialize2 <- function(..., check = TRUE)
{
    if (!isTRUEorFALSE(check)) {
        stop("'check' must be TRUE or FALSE")
    }
    disableValidity <- S4Vectors:::disableValidity
    old_val <- disableValidity()
    on.exit(disableValidity(old_val))
    disableValidity(!check)
    initialize(...)
}

#' @importFrom bit64 as.integer64 is.integer64
.has.row_number <- function(x) {
    if (length(x@key) == 1L) {
        key1 <- x@key[[1L]]
        is.integer64(key1) && (length(key1) == 2L) && is.na(key1[1L]) && (key1[2L] <= as.integer64(0L))
    } else {
        FALSE
    }
}

#' @importFrom bit64 NA_integer64_
#' @importFrom dplyr n pull summarize
.key.row_number <- function(conn) {
    c(NA_integer64_, - pull(summarize(conn, n = n())))
}

#' @export
#' @importClassesFrom BiocGenerics OutOfMemoryObject
#' @importClassesFrom S4Vectors RectangularData
setClass("DuckDBTable", contains = c("RectangularData", "OutOfMemoryObject"),
    slots = c(conn = "tbl_duckdb_connection", key = "list", fact = "LanguageList"))

#' @importFrom S4Vectors setValidity2
setValidity2("DuckDBTable", function(x) {
    msg <- NULL
    if (is.null(names(x@key))) {
        msg <- c(msg, "'key' slot must be a named list")
    }
    if (!all(names(x@key) %in% colnames(x@conn))) {
        msg <- c(msg, "all names in 'key' slot must match column names in 'conn'")
    }
    for (i in seq_along(x@key)) {
        if (!is.atomic(x@key[[i]])) {
            msg <- c(msg, "all elements in 'key' slot must be atomic")
            break
        }
    }
    if (is.null(names(x@fact))) {
        msg <- c(msg, "'fact' slot must be a named LanguageList object")
    }
    if (length(intersect(names(x@key), names(x@fact)))) {
        msg <- c(msg, "names in 'key' and 'fact' slots must be unique")
    }
    if (is.null(msg)) {
        TRUE
    } else {
        msg
    }
})

#' @export
#' @importFrom BiocGenerics dbconn
setMethod("dbconn", "DuckDBTable", function(x) x@conn)

#' @export
setMethod("nkey", "DuckDBTable", function(x) length(x@key))

#' @export
setMethod("nkeydim", "DuckDBTable", function(x) {
    if (.has.row_number(x)) {
        abs(x@key[[1L]][2L])
    } else {
        lengths(x@key, use.names = FALSE)
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
setMethod("ncol", "DuckDBTable", function(x) length(x@fact))

#' @export
setMethod("keynames", "DuckDBTable", function(x) names(x@key))

#' @export
#' @importFrom dplyr pull select
setMethod("keydimnames", "DuckDBTable", function(x) {
    if (.has.row_number(x)) {
        list(as.character(pull(select(x@conn, !!as.name(names(x@key))))))
    } else {
        lapply(x@key, function(y) names(y) %||% as.character(y))
    }
})

#' @export
setReplaceMethod("keydimnames", "DuckDBTable", function(x, value) {
    if (!is.list(value)) {
        stop("'value' must be a list of vectors")
    }
    key <- x@key
    if (is.null(names(value))) {
        if (length(value) != length(key)) {
            stop("if 'value' is unnamed, then it must match length of 'key'")
        }
        names(value) <- names(key)
    }
    for (i in names(value)) {
        names(key[[i]]) <- value[[i]]
    }
    initialize2(x, key = key, check = FALSE)
})

#' @export
#' @importFrom BiocGenerics rownames
setMethod("rownames", "DuckDBTable", function(x, do.NULL = TRUE, prefix = "row") {
    if (length(x@key) == 1L) {
        keydimnames(x)[[1L]]
    } else {
        stop("rownames is not supported for multi-dimensional keys")
    }
})

#' @export
#' @importFrom BiocGenerics colnames
setMethod("colnames", "DuckDBTable", function(x, do.NULL = TRUE, prefix = "col") names(x@fact))

#' @export
#' @importFrom BiocGenerics colnames<-
setReplaceMethod("colnames", "DuckDBTable", function(x, value) {
    fact <- x@fact
    names(fact) <- value
    initialize2(x, fact = fact, check = FALSE)
})

#' @importFrom bit64 is.integer64
.get_type <- function(column) {
    if (is.integer64(column)) {
        "integer64"
    } else if (inherits(column, "Date")) {
        "Date"
    } else if (is.list(column)) {
        "raw"
    } else {
        DelayedArray::type(column)
    }
}

.cast_fact <- function(fact, value) {
    if (is.null(names(value)) && (length(value) == length(fact))) {
        names(value) <- names(fact)
    }
    for (j in names(value)) {
        cast <- switch(value[j],
                       logical = "as.logical",
                       integer = "as.integer",
                       integer64 = "as.integer64",
                       double = "as.double",
                       character = "as.character",
                       stop("'type' must be one of 'logical', 'integer', 'integer64', 'double', or 'character'"))
        fact[[j]] <- call(cast, fact[[j]])
    }
    fact
}

#' @export
setGeneric("coltypes", function(x) standardGeneric("coltypes"))

#' @export
setMethod("coltypes", "DuckDBTable", function(x) {
    i <- sapply(keynames(x), function(x) character(), simplify = FALSE)
    empty <- .subset_DuckDBTable(x, i, drop = FALSE)
    vapply(as.data.frame(empty)[names(x@fact)], .get_type, character(1L))
})

#' @export
setGeneric("coltypes<-", function(x, value) standardGeneric("coltypes<-"))

#' @export
setReplaceMethod("coltypes", "DuckDBTable", function(x, value) {
    fact <- .cast_fact(x@fact, value)
    initialize2(x, fact = fact, check = FALSE)
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
    fact <- x@fact
    ctypes <- coltypes(x)
    for (j in names(ctypes)) {
        fact[[j]] <- switch(ctypes[j],
                            logical = fact[[j]],
                            integer =,
                            integer64 =,
                            double =,
                            character =,
                            raw = call("!=", fact[[j]], .zeros[[ctypes[j]]]),
                            TRUE)
    }
    initialize2(x, fact = fact, check = FALSE)
})

#' @export
#' @importFrom SparseArray nzcount
setMethod("nzcount", "DuckDBTable", function(x) {
    tbl <- is_nonzero(x)
    coltypes(tbl) <- rep.int("integer", ncol(tbl))
    fact <- LanguageList(nonzero = Reduce(function(x, y) call("+", x, y), tbl@fact))
    tbl <- initialize2(tbl, fact = fact, check = FALSE)
    sum(tbl)
})

#' @export
#' @importFrom S4Arrays is_sparse
setMethod("is_sparse", "DuckDBTable", function(x) {
    (ncol(x) == 1L) && ((nzcount(x) / nrow(x)) < 0.5)
})

#' @importFrom dplyr distinct filter pull select
.subset_DuckDBTable <- function(x, i, j, ..., drop = TRUE) {
    conn <- x@conn
    fact <- x@fact
    if (!missing(j)) {
        fact <- fact[j]
    }

    key <- x@key
    if (!missing(i)) {
        if (!is.list(i) || is.null(names(i))) {
            stop("'i' must be a named list")
        }
        for (k in intersect(names(key), names(i))) {
            sub <- i[[k]]
            if (is.character(sub) && is.null(names(key[[k]]))) {
                key[[k]] <- sub
            } else if (is.atomic(sub)) {
                key[[k]] <- key[[k]][sub]
            } else if (is(sub, "ParquetColumn") &&
                       is.logical(as.vector(head(sub, 0L))) &&
                       isTRUE(all.equal(as(x, "DuckDBTable"), sub@table))) {
                keep <- sub@table@fact[[1L]]
                conn <- filter(conn, !!keep)
                if (.has.row_number(x)) {
                    key[[1L]] <- .key.row_number(conn)
                } else {
                    for (kname in names(key)) {
                        kdnames <- pull(distinct(select(conn, !!as.name(kname))))
                        key[[kname]] <- key[[kname]][match(kdnames, key[[kname]])]
                    }
                }
            } else {
                stop("unsupported 'i' for row subsetting")
            }
        }
    }

    initialize2(x, conn = conn, key = key, fact = fact, ..., check = FALSE)
}

#' @export
setMethod("[", "DuckDBTable", .subset_DuckDBTable)

#' @export
#' @importFrom S4Vectors bindCOLS
setMethod("bindCOLS", "DuckDBTable",
function(x, objects = list(), use.names = TRUE, ignore.mcols = FALSE, check = TRUE) {
    fact <- x@fact

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
        fact <- c(fact, obj@fact)
    }
    names(fact) <- make.unique(names(fact), sep = "_")
    initialize2(x, fact = fact, check = FALSE)
})

#' @exportS3Method base::all.equal
all.equal.DuckDBTable <- function(target, current, check.fact = FALSE, ...) {
    if (!is(current, "DuckDBTable")) {
        return("current is not a DuckDBTable")
    }
    target <- as(target, "DuckDBTable")
    current <- as(current, "DuckDBTable")
    if (!check.fact) {
        target <- target[, integer()]
        current <- current[, integer()]
    }
    callGeneric(unclass(target), unclass(current), ...)
}

#' @importFrom S4Vectors new2
#' @importFrom stats setNames
.Ops.DuckDBTable <- function(.Generic, conn, key, fin1, fin2, fout) {
    fact <- LanguageList(setNames(Map(function(x, y) call(.Generic, x, y), fin1, fin2), fout))
    new2("DuckDBTable", conn = conn, key = key, fact = fact, check = FALSE)
}

#' @export
setMethod("Ops", c(e1 = "DuckDBTable", e2 = "DuckDBTable"), function(e1, e2) {
    if (!isTRUE(all.equal(e1, e2)) || ((ncol(e1) > 1L) && (ncol(e2) > 1L) && (ncol(e1) != ncol(e2)))) {
        stop("can only perform arithmetic operations with compatible objects")
    }
    comb <- cbind(e1, e2)
    fin1 <- head(comb@fact, ncol(e1))
    fin2 <- tail(comb@fact, ncol(e2))
    if (ncol(e1) >= ncol(e2)) {
        fout <- colnames(e1)
    } else {
        fout <- colnames(e2)
    }
    .Ops.DuckDBTable(.Generic, conn = comb@conn, key = comb@key, fin1 = fin1, fin2 = fin2, fout = fout)
})

#' @export
setMethod("Ops", c(e1 = "DuckDBTable", e2 = "atomic"), function(e1, e2) {
    if (length(e2) != 1L) {
        stop("can only perform binary operations with a scalar value")
    }
    .Ops.DuckDBTable(.Generic, conn = e1@conn, key = e1@key, fin1 = e1@fact, fin2 = e2, fout = colnames(e1))
})

#' @export
setMethod("Ops", c(e1 = "atomic", e2 = "DuckDBTable"), function(e1, e2) {
    if (length(e1) != 1L) {
        stop("can only perform binary operations with a scalar value")
    }
    .Ops.DuckDBTable(.Generic, conn = e2@conn, key = e2@key, fin1 = e1, fin2 = e2@fact, fout = colnames(e2))
})

#' @export
#' @importFrom S4Vectors endoapply
setMethod("Math", "DuckDBTable", function(x) {
    fact <-
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
                endoapply(x@fact, function(j) call(.Generic, j))
             },
             stop("unsupported Math operator: ", .Generic))
    initialize2(x, fact = fact, check = FALSE)
})

#' @importFrom dplyr pull summarize
.pull.aggregagte <- function(x, fun, na.rm = FALSE) {
    if (length(x@fact) != 1L) {
        stop("aggregation requires a single fact")
    }
    if (na.rm) {
        aggr <- call(fun, x@fact[[1L]], na.rm = TRUE)
    } else {
        aggr <- call(fun, x@fact[[1L]])
    }
    pull(summarize(x@conn, !!aggr))
}

#' @export
setMethod("Summary", "DuckDBTable", function(x, ..., na.rm = FALSE) {
    if (.Generic == "range") {
        if (length(x@fact) != 1L) {
            stop("aggregation requires a single fact")
        }
        aggr <- list(min = call("min", x@fact[[1L]], na.rm = TRUE),
                     max = call("max", x@fact[[1L]], na.rm = TRUE))
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
    if (length(x@fact) != 1L) {
        stop("aggregation requires a single fact")
    }
    if (!isSingleNumber(type) || !(type %in% c(1L, 7L))) {
        stop("'type' must be 1 or 7")
    } else if (type == 1L) {
        fun <- "quantile_disc"
    } else {
        fun <- "quantile_cont"
    }
    aggr <- lapply(probs, function(p) call(fun, x@fact[[1L]], p))
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
#' @importFrom BiocGenerics as.data.frame
#' @importFrom dplyr filter mutate select
setMethod("as.data.frame", "DuckDBTable", function(x, row.names = NULL, optional = FALSE, ...) {
    conn <- x@conn
    key <- x@key
    fact <- as.list(x@fact)

    if (!.has.row_number(x)) {
        for (i in names(key)) {
            set <- key[[i]]
            conn <- filter(conn, !!as.name(i) %in% set)
        }
    }

    conn <- mutate(conn, !!!fact)
    conn <- select(conn, c(names(key), names(fact)))

    # Allow for 1 extra row to check for duplicate keys
    length <- nrow(x) + 1L
    conn <- head(conn, n = length)

    df <- as.data.frame(conn)[, c(names(key), names(fact))]
    if (anyDuplicated(df[, names(key)])) {
        stop("duplicate keys found in Parquet data")
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

#' @export
#' @importFrom dplyr distinct mutate pull select
#' @importFrom S4Vectors new2
#' @importFrom stats setNames
#' @rdname DuckDBTable
DuckDBTable <- function(conn, key, fact = setdiff(colnames(conn), names(key)), type = NULL, ...) {
    # Acquire the connection if it is a string
    if (is.character(conn)) {
        conn <- acquireTable(conn, ...)
    }
    if (!inherits(conn, "tbl_duckdb_connection")) {
        stop("'conn' must be a 'tbl_duckdb_connection' object")
    }

    # Ensure 'key' is a named list of vectors
    if (missing(key)) {
        key <- tail(make.unique(c(colnames(conn), "row_number"), sep = "_"), 1L)
        key <- setNames(list(call("row_number")), key)
        conn <- mutate(conn, !!!key)
        key[[1L]] <- .key.row_number(conn)
    } else if (is.character(key)) {
        key <- sapply(key, function(x) NULL, simplify = FALSE)
    }
    if (!is.list(key) || is.null(names(key))) {
        stop("'key' must be a character vector or a named list of vectors")
    }
    for (k in names(key)) {
        if (is.null(key[[k]])) {
            key[[k]] <- pull(distinct(select(conn, !!as.name(k))))
        }
    }

    # Ensure 'fact' is a named LanguageList object
    if (is.character(fact)) {
        fact <- sapply(fact, as.name, simplify = FALSE)
        if (!is.null(type)) {
            if (is.null(names(type)) || length(setdiff(names(type), names(fact)))) {
                stop("all names in 'type' must have a corresponding name in 'fact'")
            }
            fact <- .cast_fact(fact, type)
        }
    }
    if (is.list(fact)) {
        fact <- LanguageList(fact)
    }
    if (is.null(names(fact))) {
        stop("'fact' must be a named LanguageList object")
    }

    new2("DuckDBTable", conn = conn, key = key, fact = fact, check = FALSE)
}
