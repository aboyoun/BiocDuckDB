#' DuckDBDataFrame objects
#'
#' @description
#' The DuckDBDataFrame class extends both \linkS4class{DuckDBTable} and
#' \linkS4class{DataFrame} to represent a DuckDB table as a
#' \linkS4class{DataFrame} object.
#'
#' @details
#' DuckDBDataFrame adds \linkS4class{DataFrame} semantics to a DuckDB table.
#' It achieves a balance between the flexibility of a tbl_duckdb_connection
#' object and the familiarity of a \linkS4class{DataFrame} object.
#'
#' @section Constructor:
#' \describe{
#'   \item{\code{DuckDBDataFrame(conn, datacols = colnames(conn), keycol = NULL, dimtbl = NULL, type = NULL)}:}{
#'     Creates a DuckDBDataFrame object.
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
#'       \item{\code{keycol}}{
#'         An optional string specifying the column name from \code{conn} that
#'         will define the foreign key in the underlying table, or a named list
#'         containing a character vector where the name of the list element
#'         defines the foreign key and the character vector set the distinct
#'         values for that key. If missing, a \code{row_number} column is
#'         created as an identifier.
#'       }
#'       \item{\code{dimtbl}}{
#'         A optional named \code{DataFrameList} that specifies the dimension
#'         table associated with the \code{keycol}. The name of the list
#'         element must match the name of the \code{keycol} list. Additionally,
#'         the \code{DataFrame} object must have row names that match the
#'         distinct values of the \code{keycol} list element and columns
#'         that define partitions in the data table for efficient querying.
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
#' In the code snippets below, \code{x} is a DuckDBDataFrame object:
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
#'   \item{\code{coltypes(x)}, \code{coltypes(x) <- value}:}{
#'     Get or set the data type of the columns; one of \code{"logical"},
#'     \code{"integer"}, \code{"integer64"}, \code{"double"}, or
#'     \code{"character"}.
#'   }
#'   \item{\code{dimtbls(x)}, \code{dimtbls(x) <- value}:}{
#'     Get or set the list of dimension tables used to define partitions for
#'     efficient queries.
#'   }
#' }
#'
#' @section Coercion:
#' \describe{
#'   \item{\code{as.data.frame(x)}:}{
#'     Coerces \code{x} to a data.frame.
#'   }
#'   \item{\code{as(from, "DFrame")}:}{
#'     Converts a DuckDBDataFrame object to a DFrame object. This process
#'     involves first loading the data into memory using \code{as.data.frame}.
#'     The resulting data.frame is then coerced into a DFrame. Additionally,
#'     any associated metadata and mcols (metadata columns) are preserved and
#'     added to the DFrame, if they exist.
#'   }
#'   \item{\code{realize(x, BACKEND = getAutoRealizationBackend())}:}{
#'     Realize an object into memory or on disk using the equivalent of
#'     \code{realize(as(x, "DFrame"), BACKEND)}.
#'   }
#' }
#'
#' @section Subsetting:
#' In the code snippets below, \code{x} is a DuckDBDataFrame object:
#' \describe{
#'   \item{\code{x[i, j, drop = TRUE]}:}{
#'     Returns either a new DuckDBDataFrame object or a DuckDBColumn if
#'     selecting a single column and \code{drop = TRUE}.
#'   }
#'   \item{\code{x[[i]]}:}{
#'     Extracts a DuckDBColumn object from \code{x}.
#'   }
#'   \item{\code{x[[i]] <- value}:}{
#'    Replaces column \code{i} in \code{x} with \code{value}.
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
#'   \item{\code{subset(x, subset, select, drop = FALSE)}:}{
#'     Return a new DuckDBDataFrame using:
#'     \describe{
#'       \item{subset}{logical expression indicating rows to keep, where missing
#'          values are taken as FALSE.}
#'        \item{select}{expression indicating columns to keep.}
#'        \item{drop}{passed on to \code{[} indexing operator.}
#'     }
#'   }
#' }
#'
#' @section Combining:
#' \describe{
#'   \item{\code{cbind(...)}:}{
#'     Creates a new DuckDBDataFrame object by concatenating the columns of the
#'     input objects.
#'     The returned DuckDBDataFrame object concatenates the metadata across the
#'     input objects.
#'     The metadata columns of the returned DuckDBDataFrame object are obtained
#'     by combining the metadata columns of the input object with
#'     \code{combineRows()}.
#'   }
#' }
#'
#' @section Displaying:
#' The \code{show()} method for DuckDBDataFrame objects obeys global options
#' \code{showHeadLines} and \code{showTailLines} for controlling the number of
#' head and tail rows and columns to display.
#'
#' @author Patrick Aboyoun, Aaron Lun
#'
#' @examples
#' # Mocking up a file:
#' tf <- tempfile(fileext = ".parquet")
#' on.exit(unlink(tf))
#' arrow::write_parquet(cbind(model = rownames(mtcars), mtcars), tf)
#'
#' # Creating our DuckDB-backed data frame:
#' df <- DuckDBDataFrame(tf, datacols = colnames(mtcars), keycol = "model")
#' df
#'
#' # Extraction yields a DuckDBColumn:
#' df$carb
#'
#' # Slicing DuckDBDataFrame objects:
#' df[,1:5]
#' df[1:5,]
#'
#' # Combining by DuckDBDataFrame and DuckDBColumn objects:
#' combined <- cbind(df, df)
#' class(combined)
#' combined2 <- cbind(df, some_new_name=df[,1])
#' class(combined2)
#'
#' @aliases
#' DuckDBDataFrame-class
#'
#' length,DuckDBDataFrame-method
#' names,DuckDBDataFrame-method
#' rownames<-,DuckDBDataFrame-method
#' colnames<-,DuckDBDataFrame-method
#' names<-,DuckDBDataFrame-method
#'
#' DuckDBDataFrame
#'
#' [[,DuckDBDataFrame-method
#' [,DuckDBDataFrame,ANY,ANY,ANY-method
#' replaceROWS,DuckDBDataFrame-method
#' replaceCOLS,DuckDBDataFrame-method
#' [[<-,DuckDBDataFrame-method
#' normalizeSingleBracketReplacementValue,DuckDBDataFrame-method
#' subset,DuckDBDataFrame-method
#'
#' cbind,DuckDBDataFrame-method
#' cbind.DuckDBDataFrame
#'
#' as.data.frame,DuckDBDataFrame-method
#' coerce,DuckDBDataFrame,DFrame-method
#' realize,DuckDBDataFrame-method
#'
#' makeNakedCharacterMatrixForDisplay,DuckDBDataFrame-method
#' show,DuckDBDataFrame-method
#'
#' @include DuckDBColumn-class.R
#' @include DuckDBTable-class.R
#'
#' @keywords classes methods
#'
#' @name DuckDBDataFrame-class
NULL

#' @export
#' @importClassesFrom S4Vectors DataFrame
setClass("DuckDBDataFrame", contains = c("DuckDBTable", "DataFrame"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

# ncol method inherited from DuckDBTable
# colnames method inherited from DuckDBTable

#' @export
setMethod("length", "DuckDBDataFrame", function(x) ncol(x))

#' @export
#' @importFrom BiocGenerics colnames
setMethod("names", "DuckDBDataFrame", function(x) colnames(x))

# nrow method inherited from DuckDBTable
# rownames method inherited from DuckDBTable

#' @export
#' @importFrom BiocGenerics rownames<-
setReplaceMethod("rownames", "DuckDBDataFrame", function(x, value) {
    if (.has_row_number(x)) {
        stop("cannot replace row numbers with rownames")
    }
    keydimnames(x) <- list(value)
    x
})

#' @export
#' @importFrom BiocGenerics colnames<-
#' @importFrom S4Vectors mcols
setReplaceMethod("colnames", "DuckDBDataFrame", function(x, value) {
    datacols <- x@datacols
    names(datacols) <- value
    mc <- mcols(x)
    if (!is.null(mc)) {
        rownames(mc) <- value
    }
    replaceSlots(x, datacols = datacols, elementMetadata = mc, check = FALSE)
})

#' @export
#' @importFrom S4Vectors mcols
setReplaceMethod("names", "DuckDBDataFrame", function(x, value) {
    colnames(x) <- value
    x
})

# dimnames<- method inherited from DataFrame

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Validity
###

#' @importFrom S4Vectors setValidity2
setValidity2("DuckDBDataFrame", function(x) {
    msg <- NULL
    if (length(x@conn) > 0L) {
        if (nkey(x) > 1L) {
            msg <- c(msg, "'keycols' slot has more than one element")
        }
    }
    msg %||% TRUE
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

#' @export
#' @importFrom S4Vectors new2
DuckDBDataFrame <-
function(conn, datacols = colnames(conn), keycol = NULL, dimtbl = NULL, type = NULL) {
    if (missing(datacols)) {
        tbl <- DuckDBTable(conn, keycols = keycol, dimtbls = dimtbl, type = type)
    } else {
        tbl <- DuckDBTable(conn, datacols = datacols, keycols = keycol,
                           dimtbls = dimtbl, type = type)
    }
    new2("DuckDBDataFrame", tbl, check = FALSE)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Subsetting
###

#' @export
#' @importFrom S4Vectors new2 mcols normalizeDoubleBracketSubscript
setMethod("[[", "DuckDBDataFrame", function(x, i, j, ...) {
    if (!missing(j)) {
        stop("list-style indexing of a DuckDBDataFrame with non-missing 'j' is not supported")
    }

    if (missing(i) || length(i) != 1L) {
        stop("expected a length-1 'i' for list-style indexing of a DuckDBDataFrame")
    }

    i <- normalizeDoubleBracketSubscript(i, x)
    column <- extractCOLS(x, i)
    new2("DuckDBColumn", table = as(column, "DuckDBTable"), metadata = as.list(mcols(column)), check = FALSE)
})

#' @export
#' @importFrom S4Vectors combineRows DataFrame mcols normalizeDoubleBracketSubscript
setReplaceMethod("[[", "DuckDBDataFrame", function(x, i, j, ..., value) {
    if (is.character(i)) {
        i2 <- i
    } else {
        i2 <- normalizeDoubleBracketSubscript(i, x, allow.nomatch = TRUE)
    }

    if (is.null(value)) {
        datacols <- x@datacols
        datacols[[i2]] <- NULL
        mc <- mcols(x)
        if (!is.null(mc)) {
            mc <- mc[-i2, , drop = FALSE]
        }
        return(replaceSlots(x, datacols = datacols, elementMetadata = mc, check = FALSE))
    }

    if (length(i2) == 1L && !is.na(i2)) {
        if (is(value, "DuckDBColumn")) {
            if (isTRUE(all.equal(as(x, "DuckDBTable"), value@table))) {
                datacols <- x@datacols
                datacols[[i2]] <- value@table@datacols[[1L]]
                mc <- mcols(x)
                if (!is.null(mc) && is.character(i) && !(i %in% colnames(x))) {
                    mc <- combineRows(mc, DataFrame(row.names = i))
                }
                return(replaceSlots(x, datacols = datacols, elementMetadata = mc, check = FALSE))
            }
        }
    }

    stop("not compatible DuckDBDataFrame and DuckDBColumn objects")
})

#' @export
setMethod("[", "DuckDBDataFrame", function(x, i, j, ..., drop = TRUE) {
    if (!missing(i)) {
        x <- extractROWS(x, i)
    }
    if (!missing(j)) {
        x <- extractCOLS(x, j)
    }
    if (missing(drop)) {
        drop <- (ncol(x) == 1L)
    }
    if (drop && (ncol(x) == 1L)) {
        x <- x[[1L]]
    }
    x
})

#' @export
#' @importFrom S4Vectors replaceROWS
setMethod("replaceROWS", "DuckDBDataFrame", function(x, i, value) {
    stop("replacement of rows in a DuckDBDataFrame is not supported")
})

#' @export
#' @importFrom stats setNames
#' @importFrom S4Vectors replaceCOLS combineRows DataFrame mcols normalizeSingleBracketSubscript
setMethod("replaceCOLS", "DuckDBDataFrame", function(x, i, value) {
    xstub <- setNames(seq_along(x), names(x))
    i2 <- normalizeSingleBracketSubscript(i, xstub, allow.append = TRUE)
    if (is(value, "DuckDBDataFrame")) {
        if (isTRUE(all.equal(x, value))) {
            datacols <- x@datacols
            datacols[i2] <- unname(value@datacols)
            if (is.character(i)) {
                names(datacols)[i2] <- i
            }
            mc <- mcols(x)
            if (!is.null(mc) && !setequal(names(datacols), rownames(mc))) {
                newnames <- setdiff(names(datacols), rownames(mc))
                mc <- combineRows(mc, DataFrame(row.names = newnames))
                mc <- mc[names(datacols), , drop = FALSE]
            }
            return(replaceSlots(x, datacols = datacols, elementMetadata = mc, check = FALSE))
        }
    }
    stop("not compatible DuckDBDataFrame objects")
})

#' @export
#' @importFrom S4Vectors new2 normalizeSingleBracketReplacementValue
setMethod("normalizeSingleBracketReplacementValue", "DuckDBDataFrame",
function(value, x) {
    if (is(value, "DuckDBColumn")) {
        return(new2("DuckDBDataFrame", value@table, check = FALSE))
    }
    callNextMethod()
})

#' @export
#' @importFrom BiocGenerics subset
setMethod("subset", "DuckDBDataFrame",
function(x, subset, select, drop = FALSE, ...) {
    if (!missing(subset)) {
        i <- eval(substitute(subset), as.env(x), parent.frame())
        x <- extractROWS(x, i)
    }
    if (!missing(select)) {
        j <- S4Vectors:::evalqForSelect(select, x, ...)
        x <- extractCOLS(x, j)
    }
    if (missing(drop)) {
        drop <- (ncol(x) == 1L)
    }
    if (drop && (ncol(x) == 1L)) {
        x <- x[[1L]]
    }
    x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Binding
###

#' @export
#' @importClassesFrom S4Vectors DFrame
#' @importFrom dplyr rename
#' @importFrom S4Vectors combineRows make_zero_col_DFrame mcols mcols<- metadata new2
cbind.DuckDBDataFrame <- function(..., deparse.level = 1) {
    objects <- list(...)

    all_mcols <- vector("list", length(objects))
    all_metadata <- vector("list", length(objects))
    has_mcols <- FALSE

    for (i in seq_along(objects)) {
        obj <- objects[[i]]

        md <- list()
        mc <- make_zero_col_DFrame(NCOL(obj))
        if (is(obj, "DuckDBColumn")) {
            table <- obj@table
            cname <- names(objects)[i]
            if (!is.null(cname)) {
                colnames(table) <- cname
            }
            objects[[i]] <- new2("DuckDBDataFrame", table, check = FALSE)
            md <- metadata(obj)
            if (length(md) > 0L) {
                has_mcols <- TRUE
                mc <- new2("DFrame", rownames = cname, nrows = 1L, listData = md, check = FALSE)
                md <- list()
            }
        } else if (is(obj, "DuckDBDataFrame")) {
            mc <- mcols(obj, use.names = FALSE) %||% mc
            if (ncol(mc) > 0L) {
                has_mcols <- TRUE
                mcols(objects[[i]]) <- NULL
            }
            md <- metadata(obj)
        }
        all_mcols[[i]] <- mc
        all_metadata[[i]] <- md
    }

    x <- objects[[1L]]
    objects <- objects[-1L]
    bound <- bindCOLS(x, objects)

    if (has_mcols) {
        all_mcols <- do.call(combineRows, all_mcols)
        rownames(all_mcols) <- colnames(bound)
    } else {
        all_mcols <- NULL
    }
    all_metadata <- do.call(c, all_metadata)

    replaceSlots(bound, elementMetadata = all_mcols, metadata = all_metadata, check = FALSE)
}

#' @export
setMethod("cbind", "DuckDBDataFrame", cbind.DuckDBDataFrame)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

#' @export
#' @importFrom BiocGenerics as.data.frame
#' @importFrom stats setNames
setMethod("as.data.frame", "DuckDBDataFrame",
function(x, row.names = NULL, optional = FALSE, ...) {
    # as.data.frame,DuckDBTable-method
    df <- callNextMethod(x, row.names = row.names, optional = optional, ...)

    # Add rownames, renaming if specified
    keyname <- names(x@keycols)
    if (is.null(names(x@keycols[[1L]]))) {
        rnames <- df[[keyname]]
    } else {
        rnames <- x@keycols[[1L]]
        rnames <- setNames(names(rnames), rnames)
        rnames <- rnames[as.character(df[[keyname]])]
    }
    rownames(df) <- rnames

    df[rownames(x), colnames(x), drop = FALSE]
})

#' @export
#' @importClassesFrom S4Vectors DFrame
#' @importFrom S4Vectors mcols mcols<- metadata metadata<-
setAs("DuckDBDataFrame", "DFrame", function(from) {
    df <- as(as.data.frame(from), "DFrame")

    metadata(df) <- metadata(from)
    mc <- mcols(from)
    if (!is.null(mc)) {
        mcols(df) <- as(mc, "DFrame")
    }

    df
})

#' @export
#' @importClassesFrom S4Vectors DFrame
#' @importFrom DelayedArray getAutoRealizationBackend realize
setMethod("realize", "DuckDBDataFrame",
function(x, BACKEND = getAutoRealizationBackend()) {
    x <- as(x, "DFrame")
    if (!is.null(BACKEND)) {
        x <- callGeneric(x, BACKEND = BACKEND)
    }
    x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Display
###

#' @importFrom S4Vectors DataFrame get_showHeadLines get_showTailLines makeNakedCharacterMatrixForDisplay
.makePrettyCharacterMatrixForDisplay <- function(x) {
    if (.has_row_number(x)) {
        nhead <- get_showHeadLines() + get_showTailLines()
        ntail <- 0L
    } else {
        nhead <- get_showHeadLines()
        ntail <- get_showTailLines()
    }

    x_ncol <- NCOL(x)
    nleft <- get_showHeadLines()
    nright <- get_showTailLines()
    is_wide <- (x_ncol > nleft + nright + 1L)
    if (is_wide) {
        x <- x[, c(1L:nleft, (x_ncol - nright + 1L):x_ncol), drop = FALSE]
    }

    x_nrow <- NROW(x)
    if (x_nrow <= nhead + ntail + 1L) {
        m <- makeNakedCharacterMatrixForDisplay(x)
        x_rownames <- rownames(x)
        if (!is.null(x_rownames)) {
            rownames(m) <- x_rownames
        }
    } else {
        x_head <- head(x, nhead)
        x_rownames <- rownames(x_head)
        if (ntail == 0L) {
            m <- rbind(makeNakedCharacterMatrixForDisplay(x_head), "...")
        } else {
            i <- c(seq_len(nhead), (x_nrow + 1L) - rev(seq_len(ntail)))
            df <- DataFrame(as.data.frame(x[i, , drop = FALSE]), check.names = FALSE)
            m <- rbind(makeNakedCharacterMatrixForDisplay(head(df, nhead)),
                       "...",
                       makeNakedCharacterMatrixForDisplay(tail(df, ntail)))
            x_rownames <- c(x_rownames, rownames(tail(x, ntail)))
        }
        rownames(m) <- S4Vectors:::make_rownames_for_RectangularData_display(x_rownames, x_nrow, nhead, ntail)
    }

    m <- rbind(sprintf("<%s>", coltypes(x)), m)

    if (is_wide) {
        m <- cbind(m[, 1L:nleft], "..." = "...", m[, (nleft + 1L):ncol(m)])
    }

    m
}

#' @export
#' @importFrom S4Vectors DataFrame makeNakedCharacterMatrixForDisplay
setMethod("makeNakedCharacterMatrixForDisplay", "DuckDBDataFrame", function(x) {
    callGeneric(DataFrame(as.data.frame(x), check.names = FALSE))
})

#' @export
#' @importFrom S4Vectors classNameForDisplay
setMethod("show", "DuckDBDataFrame", function(object) {
    x_nrow <- as.double(nrow(object))
    x_ncol <- ncol(object)

    cat(classNameForDisplay(object), " with ",
        x_nrow, " row", ifelse(x_nrow == 1L, "", "s"), " and ",
        x_ncol, " column", ifelse(x_ncol == 1L, "", "s"), "\n", sep = "")

    if (x_nrow != 0L && x_ncol != 0L) {
        m <- .makePrettyCharacterMatrixForDisplay(object)
        print(m, quote = FALSE, right = TRUE)
    }

    invisible(NULL)
})
