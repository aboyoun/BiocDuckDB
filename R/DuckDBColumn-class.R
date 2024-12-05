#' DuckDBColumn objects
#'
#' @description
#' The DuckDBColumn class extends \linkS4class{Vector} to represent a column
#' extracted from a \linkS4class{DuckDBDataFrame} object.
#'
#' @section Accessors:
#' In the code snippets below, \code{x} is a DuckDBColumn object:
#' \describe{
#'   \item{\code{length(x)}:}{
#'     Get the number of elements in \code{x}.
#'   }
#'   \item{\code{names(x)}:}{
#'     Get the names of the elements of \code{x}.
#'   }
#'   \item{\code{type(x)}:}{
#'     Get the data type of the elements of \code{x}.
#'   }
#' }
#' @section Coercion:
#' \describe{
#'   \item{\code{as.vector(x)}:}{
#'     Coerces \code{x} to a \code{vector}.
#'   }
#' }
#'
#' @section Subsetting:
#' In the code snippets below, \code{x} is a DuckDBColumn object:
#' \describe{
#'   \item{\code{x[i]}:}{
#'     Returns either a DuckDBColumn object containing the selected elements.
#'   }
#'   \item{\code{head(x, n = 6L)}:}{
#'     If \code{n} is non-negative, returns the first n elements of \code{x}.
#'     If \code{n} is negative, returns all but the last \code{abs(n)} elements
#'     of \code{x}.
#'   }
#'   \item{\code{tail(x, n = 6L)}:}{
#'     If \code{n} is non-negative, returns the last n elements of \code{x}.
#'     If \code{n} is negative, returns all but the first \code{abs(n)} elements
#'     of \code{x}.
#'   }
#' }
#'
#' @author Patrick Aboyoun
#'
#' @aliases
#' DuckDBColumn-class
#'
#' dbconn,DuckDBColumn-method
#' tblconn,DuckDBColumn-method
#' length,DuckDBColumn-method
#' names,DuckDBColumn-method
#' type,DuckDBColumn-method
#' type<-,DuckDBColumn-method
#'
#' extractROWS,DuckDBColumn,ANY-method
#' head,DuckDBColumn-method
#' tail,DuckDBColumn-method
#'
#' as.vector,DuckDBColumn-method
#'
#' show,DuckDBColumn-method
#' showAsCell,DuckDBColumn-method
#'
#' @include DuckDBTable-class.R
#'
#' @keywords classes methods
#'
#' @name DuckDBColumn-class
NULL

#' @export
#' @importClassesFrom S4Vectors Vector
setClass("DuckDBColumn", contains = "Vector", slots = c(table = "DuckDBTable"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

#' @export
#' @importFrom BiocGenerics dbconn
setMethod("dbconn", "DuckDBColumn", function(x) callGeneric(x@table))

#' @export
setMethod("tblconn", "DuckDBColumn", function(x) callGeneric(x@table))

setMethod(".keycols", "DuckDBColumn", function(x) callGeneric(x@table))

setMethod(".has_row_number", "DuckDBColumn", function(x) callGeneric(x@table))

#' @export
setMethod("length", "DuckDBColumn", function(x) nrow(x@table))

#' @export
setMethod("names", "DuckDBColumn", function(x) {
    table <- x@table
    if (length(table@conn) == 0L) {
        NULL
    } else {
        keydimnames(table)[[1L]]
    }
})

#' @export
#' @importFrom BiocGenerics type
setMethod("type", "DuckDBColumn", function(x) {
    unname(coltypes(x@table))
})

#' @export
#' @importFrom BiocGenerics type<-
setReplaceMethod("type", "DuckDBColumn", function(x, value) {
    table <- x@table
    coltypes(table) <- value
    replaceSlots(x, table = table, check = FALSE)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Validity
###

#' @importFrom S4Vectors isTRUEorFALSE setValidity2
setValidity2("DuckDBColumn", function(x) {
    msg <- NULL
    table <- x@table
    if (length(table@conn) > 0L) {
        if (ncol(table) != 1L) {
            msg <- c(msg, "'table' slot must be a single-column DuckDBTable")
        }
        if (nkey(table) != 1L) {
            msg <- c(msg, "'table' slot must have a 'keycols' with a named list containing a single named character vector")
        }
    }
    msg %||% TRUE
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Subsetting
###

#' @export
setMethod("extractROWS", "DuckDBColumn", function(x, i) {
    if (missing(i)) {
        return(x)
    }
    if (is(i, "DuckDBColumn")) {
        i <- i@table
    }
    i <- setNames(list(i), names(x@table@keycols))
    replaceSlots(x, table = .subset_DuckDBTable(x@table, i = i), check = FALSE)
})

#' @export
#' @importFrom S4Vectors head isSingleNumber
setMethod("head", "DuckDBColumn", function(x, n = 6L, ...) {
    if (!isSingleNumber(n)) {
        stop("'n' must be a single number")
    }
    if (.has_row_number(x)) {
        return(replaceSlots(x, table = .head_conn(x@table, n), check = FALSE))
    }
    n <- as.integer(n)
    len <- length(x)
    if (n < 0) {
        n <- max(0L, len + n)
    }
    if (n > len) {
        x
    } else {
        extractROWS(x, seq_len(n))
    }
})

#' @export
#' @importFrom S4Vectors isSingleNumber tail
setMethod("tail", "DuckDBColumn", function(x, n = 6L, ...) {
    if (!isSingleNumber(n)) {
        stop("'n' must be a single number")
    }
    if ((n > 0L) && .has_row_number(x)) {
        stop("tail requires a keycols to be efficient")
    }
    n <- as.integer(n)
    len <- length(x)
    if (n < 0) {
        n <- max(0L, len + n)
    }
    if (n > len) {
        x
    } else {
        extractROWS(x, (len + 1L) - rev(seq_len(n)))
    }
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

#' @export
#' @importFrom BiocGenerics as.vector
#' @importFrom stats setNames
setMethod("as.vector", "DuckDBColumn", function(x, mode = "any") {
    df <- as.data.frame(x@table)
    vec <- setNames(df[[colnames(x@table)]], df[[names(x@table@keycols)]])
    vec <- vec[rownames(x@table)]
    if (mode != "any") {
        storage.mode(vec) <- mode
    }
    vec
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Display
###

#' @export
#' @importFrom S4Vectors classNameForDisplay
setMethod("show", "DuckDBColumn", function(object) {
    len <- length(object)
    cat(sprintf("%s of length %s\n", classNameForDisplay(object), len))
    if (length(object@table@conn) == 0L) {
        return(invisible(NULL))
    }
    if (.has_row_number(object)) {
        n1 <- 5L
        n2 <- 0L
    } else {
        n1 <- 3L
        n2 <- 2L
    }
    if (len <= n1 + n2 + 1L) {
        vec <- as.vector(object)
    } else {
        if (n2 == 0L) {
            vec <- as.vector(head(object, n1))
            if (is.character(vec)) {
                vec <- setNames(sprintf("\"%s\"", vec), names(vec))
            }
            vec <- format(vec, justify = "right")
            vec <- c(vec, "..." = "...")
        } else {
            i <- c(seq_len(n1), (len + 1L) - rev(seq_len(n2)))
            vec <- as.vector(object[i])
            if (is.character(vec)) {
                vec <- setNames(sprintf("\"%s\"", vec), names(vec))
            }
            vec <- format(vec, justify = "right")
            vec <- c(head(vec, n1), "..." = "...", tail(vec, n2))
        }
    }
    print(vec, quote = FALSE)
    invisible(NULL)
})

#' @export
#' @importFrom S4Vectors showAsCell
setMethod("showAsCell", "DuckDBColumn", function(object) {
    callGeneric(as.vector(object@table))
})
