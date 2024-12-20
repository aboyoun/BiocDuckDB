#' DuckDBTable objects
#'
#' @description
#' The DuckDBTable class extends the \linkS4class{RectangularData} virtual
#' class for DuckDB tables by wrapping a tbl_duckdb_connection object.
#'
#' @details
#' The DuckDBTable class provides a way to define a DuckDB table as a
#' \linkS4class{RectangularData} object. It supports \emph{standard 2D API}
#' such as \code{dim()}, \code{nrow()}, \code{ncol()}, \code{dimnames()},
#' \code{x[i, j]} and \code{cbind()}, but does not support \code{rbind()}.
#'
#' @section Constructor:
#' \describe{
#'   \item{\code{DuckDBTable(conn, datacols = colnames(conn), keycols = NULL, type = NULL)}:}{
#'     Creates a DuckDBTable object.
#'     \describe{
#'       \item{\code{conn}}{
#'         Either a character vector containing the paths to parquet, csv, or
#'         gzipped csv data files; a string that defines a duckdb \code{read_*}
#'         data source; a DuckDBDataFrame object; or a tbl_duckdb_connection
#'         object.
#'       }
#'       \item{\code{datacols}}{
#'         Either a character vector of column names from \code{conn} or a
#'         named \code{expression} that will be evaluated in the context of
#'         \code{conn} that defines the data.
#'       }
#'       \item{\code{keycols}}{
#'         An optional character vector of column names from \code{conn} that
#'         will define the primary key, or a named list of character vectors
#'         where the names of the list define the key and the character vectors
#'         set the distinct values for the key. If missing, a \code{row_number}
#'         column is created as an identifier.
#'       }
#'       \item{\code{type}}{
#'         An optional named character vector where the names specify the
#'         column names and the values specify the column type; one of
#'         \code{"logical"}, \code{"integer"}, \code{"integer64"},
#'         \code{"double"}, or \code{"character"}.
#'       }
#'     }
#'   }
#' }
#'
#' @section Accessors:
#' In the code snippets below, \code{x} is a DuckDBTable object:
#' \describe{
#'   \item{\code{dim(x)}:}{
#'     Length two integer vector defined as \code{c(nrow(x), ncol(x))}.
#'   }
#'   \item{\code{nrow(x)}, \code{ncol(x)}:}{
#'     Get the number of rows and columns, respectively.
#'   }
#'   \item{\code{NROW(x)}, \code{NCOL(x)}:}{
#'     Same as \code{nrow(x)} and \code{ncol(x)}, respectively.
#'   }
#'   \item{\code{dimnames(x)}:}{
#'     Length two list of character vectors defined as
#'     \code{list(rownames(x), colnames(x))}.
#'   }
#'   \item{\code{rownames(x)}, \code{colnames(x)}:}{
#'     Get the names of the rows and columns, respectively.
#'   }
#' }
#'
#' @section Coercion:
#' \describe{
#'   \item{\code{as.data.frame(x)}:}{
#'     Coerces \code{x} to a data.frame.
#'   }
#' }
#'
#' @section Subsetting:
#' In the code snippets below, \code{x} is a DuckDBTable object:
#' \describe{
#'   \item{\code{x[i, j]}:}{
#'     Return a new DuckDBTable of the same class as \code{x} made of the
#'     selected rows and columns.
#'   }
#'   \item{\code{head(x, n = 6L)}:}{
#'     If \code{n} is non-negative, returns the first n rows of \code{x}.
#'     If \code{n} is negative, returns all but the last \code{abs(n)} rows of
#'     \code{x}.
#'   }
#'   \item{\code{tail(x, n = 6L)}:}{
#'     If \code{n} is non-negative, returns the last n rows of \code{x}.
#'     If \code{n} is negative, returns all but the first \code{abs(n)} rows of
#'     \code{x}.
#'   }
#'   \item{\code{subset(x, subset, select)}:}{
#'     Return a new DuckDBTable using:
#'     \describe{
#'       \item{subset}{logical expression indicating rows to keep, where missing
#'          values are taken as FALSE.}
#'        \item{select}{expression indicating columns to keep.}
#'     }
#'   }
#' }
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
#'
#' dbconn,DuckDBTable-method
#' tblconn,DuckDBTable-method
#' nrow,DuckDBTable-method
#' ncol,DuckDBTable-method
#' rownames,DuckDBTable-method
#' colnames,DuckDBTable-method
#' colnames<-,DuckDBTable-method
#' coltypes
#' coltypes,DuckDBTable-method
#' coltypes<-
#' coltypes<-,DuckDBTable-method
#'
#' DuckDBTable
#'
#' all.equal.DuckDBTable
#'
#' [,DuckDBTable,ANY,ANY,ANY-method
#' extractROWS,DuckDBTable,ANY-method
#' extractCOLS,DuckDBTable-method
#' head,DuckDBTable-method
#' tail,DuckDBTable-method
#' subset,DuckDBTable-method
#'
#' bindROWS,DuckDBTable-method
#' bindCOLS,DuckDBTable-method
#'
#' as.data.frame,DuckDBTable-method
#' as.env,DuckDBTable-method
#'
#' show,DuckDBTable-method
#'
#' @include DuckDBConnection.R
#' @include keynames.R
#' @include tblconn.R
#'
#' @keywords classes methods
#'
#' @name DuckDBTable-class
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

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

#' @export
#' @importFrom BiocGenerics dbconn
setMethod("dbconn", "DuckDBTable", function(x) x@conn$src$con)

#' @export
#' @importFrom dplyr filter
#' @importFrom S4Vectors isTRUEorFALSE
setMethod("tblconn", "DuckDBTable", function(x, filter = TRUE) {
    if (!isTRUEorFALSE(filter)) {
        stop("'filter' must be TRUE or FALSE")
    }
    conn <- x@conn
    if (filter && !.has_row_number(x)) {
        keycols <- x@keycols
        for (i in names(keycols)) {
            set <- keycols[[i]]
            conn <- filter(conn, !!as.name(i) %in% set)
        }
    }
    conn
})

setGeneric(".keycols", function(x) standardGeneric(".keycols"))

setMethod(".keycols", "DuckDBTable", function(x) x@keycols)

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

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Validity
###

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
    msg %||% TRUE
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

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
    if (length(keycols) == 0L) {
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

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Comparison
###

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

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Subsetting
###

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
#' @importFrom S4Vectors extractROWS
#' @importFrom stats setNames
setMethod("extractROWS", "DuckDBTable", function(x, i) {
    if (missing(i)) {
        return(x)
    }
    i <- setNames(list(i), names(x@keycols))
    .subset_DuckDBTable(x, i = i)
})

#' @export
#' @importFrom stats setNames
#' @importFrom S4Vectors extractCOLS mcols normalizeSingleBracketSubscript
setMethod("extractCOLS", "DuckDBTable", function(x, i) {
    if (missing(i)) {
        return(x)
    }
    xstub <- setNames(seq_along(x), names(x))
    i <- normalizeSingleBracketSubscript(i, xstub)
    if (anyDuplicated(i)) {
        stop("cannot extract duplicate columns in a DuckDBDataFrame")
    }
    mc <- extractROWS(mcols(x), i)
    .subset_DuckDBTable(x, j = i, elementMetadata = mc)
})

.head_conn <- function(x, n) {
    conn <- head(x@conn, n)
    keycols <- x@keycols
    keycols[[1L]] <- .keycols.row_number(conn)
    replaceSlots(x, conn = conn, keycols = keycols, check = FALSE)
}

#' @export
#' @importFrom S4Vectors head isSingleNumber
setMethod("head", "DuckDBTable", function(x, n = 6L, ...) {
    if (!isSingleNumber(n)) {
        stop("'n' must be a single number")
    }
    if (.has_row_number(x)) {
        return(.head_conn(x, n))
    }
    n <- as.integer(n)
    nr <- nrow(x)
    if (n < 0) {
        n <- max(0L, nr + n)
    }
    if (n > nr) {
        x
    } else {
        extractROWS(x, seq_len(n))
    }
})

#' @export
#' @importFrom S4Vectors isSingleNumber tail
setMethod("tail", "DuckDBTable", function(x, n = 6L, ...) {
    if (!isSingleNumber(n)) {
        stop("'n' must be a single number")
    }
    if ((n > 0L) && .has_row_number(x)) {
        stop("tail requires a keycols to be efficient")
    }
    n <- as.integer(n)
    nr <- nrow(x)
    if (n < 0) {
        n <- max(0L, nr + n)
    }
    if (n > nr) {
        x
    } else {
        extractROWS(x, (nr + 1L) - rev(seq_len(n)))
    }
})

#' @export
#' @importFrom BiocGenerics subset
setMethod("subset", "DuckDBTable",
function(x, subset, select, drop = FALSE, ...) {
    if (!missing(subset)) {
        i <- eval(substitute(subset), as.env(x), parent.frame())
        x <- extractROWS(x, i)
    }
    if (!missing(select)) {
        j <- S4Vectors:::evalqForSelect(select, x, ...)
        x <- extractCOLS(x, j)
    }
    x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Binding
###

#' @export
#' @importFrom S4Vectors classNameForDisplay bindROWS
setMethod("bindROWS", "DuckDBTable",
function(x, objects = list(), use.names = TRUE, ignore.mcols = FALSE, check = TRUE) {
    stop(sprintf("binding rows to a %s is not supported", classNameForDisplay(x)))
})

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

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

#' @export
#' @importFrom BiocGenerics as.data.frame
#' @importFrom dplyr mutate select
setMethod("as.data.frame", "DuckDBTable",
function(x, row.names = NULL, optional = FALSE, ...) {
    conn <- tblconn(x)
    keycols <- x@keycols
    datacols <- as.list(x@datacols)

    # Mutate and select the data columns
    conn <- mutate(conn, !!!as.list(datacols))
    conn <- select(conn, c(names(keycols), names(datacols)))

    # Allow for 1 extra row to check for duplicate keys, up to integer.max
    length <- as.integer(min(nrow(x) + 1L, .Machine$integer.max))
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

#' @export
#' @importFrom S4Vectors as.env
setMethod("as.env", "DuckDBTable",
function(x, enclos = parent.frame(2), tform = identity) {
    S4Vectors:::makeEnvForNames(x, colnames(x), enclos, tform)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Display
###

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
