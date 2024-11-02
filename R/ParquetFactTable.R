#' ParquetFactTable objects
#'
#' @description
#' ParquetFactTable is a low-level helper class for representing a
#' pointer to a Parquet fact table.
#'
#' @param conn Either a string containing the path to the Parquet data or a
#' \code{tbl_duckdb_connection} object.
#' @param key Either a character vector or a named list of character vectors
#' containing the names of the columns in the Parquet data that specify
#' the primary key of the array.
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
#' tbl <- ParquetFactTable(tf, key = c("Class", "Sex", "Age", "Survived"), fact = "fate")
#'
#' @aliases
#' ParquetFactTable-class
#' [,ParquetFactTable,ANY,ANY,ANY-method
#' all.equal.ParquetFactTable
#' as.data.frame,ParquetFactTable-method
#' bindCOLS,ParquetFactTable-method
#' colnames,ParquetFactTable-method
#' colnames<-,ParquetFactTable-method
#' dbconn,ParquetFactTable-method
#' ncol,ParquetFactTable-method
#' nrow,ParquetFactTable-method
#' rownames,ParquetFactTable-method
#' Ops,ParquetFactTable,ParquetFactTable-method
#' Ops,ParquetFactTable,atomic-method
#' Ops,atomic,ParquetFactTable-method
#' Math,ParquetFactTable-method
#' Summary,ParquetFactTable-method
#' mean,ParquetFactTable-method
#' median.ParquetFactTable
#' quantile.ParquetFactTable
#' var,ParquetFactTable,ANY-method
#' sd,ParquetFactTable-method
#' mad,ParquetFactTable-method
#'
#' @include LanguageList.R
#' @include duckdb_connection.R
#' @include acquireDataset.R
#' @include keynames.R
#'
#' @name ParquetFactTable
NULL

#' @export
#' @importClassesFrom BiocGenerics OutOfMemoryObject
#' @importClassesFrom IRanges CharacterList
#' @importClassesFrom S4Vectors RectangularData
setClass("ParquetFactTable", contains = c("RectangularData", "OutOfMemoryObject"),
    slots = c(conn = "tbl_duckdb_connection", key = "CharacterList", fact = "LanguageList"))

#' @importFrom S4Vectors setValidity2
setValidity2("ParquetFactTable", function(x) {
    msg <- NULL
    if (is.null(names(x@key))) {
        msg <- c(msg, "'key' slot must be a named CharacterList")
    }
    if (!all(names(x@key) %in% colnames(x@conn))) {
        msg <- c(msg, "all names in 'key' slot must match column names in 'conn'")
    }
    for (i in seq_along(x@key)) {
        if (is.null(names(x@key[[i]]))) {
            msg <- c(msg, "all elements in 'key' slot must be named character vectors")
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
setMethod("dbconn", "ParquetFactTable", function(x) x@conn)

#' @export
setMethod("nkey", "ParquetFactTable", function(x) length(x@key))

#' @export
setMethod("nkeydim", "ParquetFactTable", function(x) lengths(x@key, use.names = FALSE))

#' @export
#' @importFrom BiocGenerics nrow
setMethod("nrow", "ParquetFactTable", function(x) as.integer(prod(nkeydim(x))))

#' @export
#' @importFrom BiocGenerics ncol
setMethod("ncol", "ParquetFactTable", function(x) length(x@fact))

#' @export
setMethod("keynames", "ParquetFactTable", function(x) names(x@key))

#' @export
setMethod("keydimnames", "ParquetFactTable", function(x) lapply(x@key, names))

#' @export
setReplaceMethod("keydimnames", "ParquetFactTable", function(x, value) {
    if (!is.list(value) || is(value, "CharacterList")) {
        stop("'value' must be a list of character vectors")
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
    initialize(x, key = key)
})

#' @export
#' @importFrom BiocGenerics rownames
setMethod("rownames", "ParquetFactTable", function(x, do.NULL = TRUE, prefix = "row") {
    if (length(x@key) == 1L) {
        names(x@key[[1L]])
    } else {
        do.call(paste, c(do.call(expand.grid, lapply(x@key, names)), list(sep = "|")))
    }
})

#' @export
#' @importFrom BiocGenerics colnames
setMethod("colnames", "ParquetFactTable", function(x, do.NULL = TRUE, prefix = "col") names(x@fact))

#' @export
#' @importFrom BiocGenerics colnames<-
setReplaceMethod("colnames", "ParquetFactTable", function(x, value) {
    fact <- x@fact
    names(fact) <- value
    initialize(x, fact = fact)
})

#' @importFrom dplyr distinct filter pull select
.subset_ParquetFactTable <- function(x, i, j, ..., drop = TRUE) {
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
            if (is.atomic(sub)) {
                key[[k]] <- key[[k]][sub]
            } else if (is(sub, "ParquetColumn") &&
                       is.logical(as.vector(head(sub, 0L))) &&
                       isTRUE(all.equal(as(x, "ParquetFactTable"), sub@table))) {
                keep <- sub@table@fact[[1L]]
                conn <- filter(conn, !!keep)
                for (kname in names(key)) {
                    kdnames <- pull(distinct(select(conn, as.name(!!kname))))
                    key[[kname]] <- key[[kname]][match(kdnames, key[[kname]])]
                }
            } else {
                stop("unsupported 'i' for row subsetting")
            }
        }
    }

    initialize(x, conn = conn, key = key, fact = fact, ...)
}

#' @export
setMethod("[", "ParquetFactTable", .subset_ParquetFactTable)

#' @export
#' @importFrom S4Vectors bindCOLS
setMethod("bindCOLS", "ParquetFactTable",
function(x, objects = list(), use.names = TRUE, ignore.mcols = FALSE, check = TRUE) {
    fact <- x@fact

    for (i in seq_along(objects)) {
        obj <- objects[[i]]
        if (!is(obj, "ParquetFactTable")) {
            stop("all objects must be of class 'ParquetFactTable'")
        }
        if (!isTRUE(all.equal(x, obj))) {
            stop("all objects must share a compatible 'ParquetFactTable' structure")
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
    initialize(x, fact = fact)
})

#' @exportS3Method base::all.equal
all.equal.ParquetFactTable <- function(target, current, check.fact = FALSE, ...) {
    if (!is(current, "ParquetFactTable")) {
        return("current is not a ParquetFactTable")
    }
    target <- as(target, "ParquetFactTable")
    current <- as(current, "ParquetFactTable")
    if (!check.fact) {
        target <- target[, integer()]
        current <- current[, integer()]
    }
    callGeneric(unclass(target), unclass(current), ...)
}

#' @importFrom S4Vectors new2
#' @importFrom stats setNames
.Ops.ParquetFactTable <- function(.Generic, conn, key, fin1, fin2, fout) {
    fact <- LanguageList(setNames(Map(function(x, y) call(.Generic, x, y), fin1, fin2), fout))
    new2("ParquetFactTable", conn = conn, key = key, fact = fact, check = FALSE)
}

#' @export
setMethod("Ops", c(e1 = "ParquetFactTable", e2 = "ParquetFactTable"), function(e1, e2) {
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
    .Ops.ParquetFactTable(.Generic, conn = comb@conn, key = comb@key, fin1 = fin1, fin2 = fin2, fout = fout)
})

#' @export
setMethod("Ops", c(e1 = "ParquetFactTable", e2 = "atomic"), function(e1, e2) {
    if (length(e2) != 1L) {
        stop("can only perform binary operations with a scalar value")
    }
    .Ops.ParquetFactTable(.Generic, conn = e1@conn, key = e1@key, fin1 = e1@fact, fin2 = e2, fout = colnames(e1))
})

#' @export
setMethod("Ops", c(e1 = "atomic", e2 = "ParquetFactTable"), function(e1, e2) {
    if (length(e1) != 1L) {
        stop("can only perform binary operations with a scalar value")
    }
    .Ops.ParquetFactTable(.Generic, conn = e2@conn, key = e2@key, fin1 = e1, fin2 = e2@fact, fout = colnames(e2))
})

#' @export
#' @importFrom S4Vectors endoapply
setMethod("Math", "ParquetFactTable", function(x) {
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
    initialize(x, fact = fact)
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
setMethod("Summary", "ParquetFactTable", function(x, ..., na.rm = FALSE) {
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
setMethod("mean", "ParquetFactTable", function(x, ...) {
    .pull.aggregagte(x, "mean", na.rm = TRUE)
})

#' @exportS3Method stats::median
#' @importFrom stats median
median.ParquetFactTable <- function(x, na.rm = FALSE, ...) {
    .pull.aggregagte(x, "median", na.rm = TRUE)
}

#' @exportS3Method stats::quantile
#' @importFrom dplyr summarize
#' @importFrom S4Vectors isSingleNumber
#' @importFrom stats quantile
quantile.ParquetFactTable <-
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
setMethod("var", "ParquetFactTable", function(x, y = NULL, na.rm = FALSE, use)  {
    if (!is.null(y)) {
        stop("covariance is not supported")
    }
    .pull.aggregagte(x, "var", na.rm = TRUE)
})

#' @export
#' @importFrom BiocGenerics sd
setMethod("sd", "ParquetFactTable", function(x, na.rm = FALSE) {
    .pull.aggregagte(x, "sd", na.rm = TRUE)
})

#' @export
#' @importFrom BiocGenerics mad
setMethod("mad", "ParquetFactTable",
function(x, center = median(x), constant = 1.4826, na.rm = FALSE, low = FALSE, high = FALSE) {
    constant * .pull.aggregagte(x, "mad")
})

#' @export
#' @importFrom BiocGenerics as.data.frame
#' @importFrom dplyr filter mutate select
setMethod("as.data.frame", "ParquetFactTable", function(x, row.names = NULL, optional = FALSE, ...) {
    conn <- x@conn
    key <- x@key
    fact <- as.list(x@fact)

    for (i in names(key)) {
        set <- key[[i]]
        conn <- filter(conn, as.character(!!as.name(i)) %in% set)
    }

    conn <- mutate(conn, !!!fact)
    conn <- select(conn, c(names(key), names(fact)))

    # Allow for 1 extra row to check for duplicate keys
    length <- prod(lengths(key, use.names = FALSE)) + 1L
    conn <- head(conn, n = length)

    df <- as.data.frame(conn)[, c(names(key), names(fact))]
    if (anyDuplicated(df[, names(key)])) {
        stop("duplicate keys found in Parquet data")
    }

    df
})

#' @export
#' @importFrom dplyr distinct pull select
#' @importFrom IRanges CharacterList
#' @importFrom S4Vectors new2
#' @rdname ParquetFactTable
ParquetFactTable <- function(conn, key, fact = setdiff(colnames(conn), names(key)), type = NULL, ...) {
    if (is.character(conn)) {
        conn <- acquireDataset(conn, ...)
    }
    if (!inherits(conn, "tbl_duckdb_connection")) {
        stop("'conn' must be a 'tbl_duckdb_connection' object")
    }

    if (is.character(key)) {
        key <- sapply(key, function(x) pull(distinct(select(conn, as.name(!!x)))), simplify = FALSE)
    }
    if (is.list(key)) {
        key <- CharacterList(key, compress = FALSE)
    }
    if (!is(key, "CharacterList") || is.null(names(key))) {
        stop("'key' must be a character vector or a named list of character vectors")
    }
    for (i in seq_along(key)) {
        nms <- names(key[[i]])
        if (is.null(nms) || anyNA(nms) || any(!nzchar(nms))) {
            names(key[[i]]) <- key[[i]]
        }
    }

    if (is.character(fact)) {
        fact <- sapply(fact, as.name, simplify = FALSE)
        if (!is.null(type)) {
            if (is.null(names(type)) || length(setdiff(names(type), names(fact)))) {
                stop("all names in 'type' must have a corresponding name in 'fact'")
            }
            for (j in names(type)) {
                cast <- switch(type[j],
                               logical = "as.logical",
                               integer = "as.integer",
                               double = "as.double",
                               character = "as.character",
                               stop("'type' must be one of 'logical', 'integer', 'double', or 'character'"))
                fact[[j]] <- call(cast, fact[[j]])
            }
        }
    }
    if (is.list(fact)) {
        fact <- LanguageList(fact)
    }
    if (is.null(names(fact))) {
        stop("'fact' must be a named LanguageList object")
    }

    new2("ParquetFactTable", conn = conn, key = key, fact = fact, check = FALSE)
}
