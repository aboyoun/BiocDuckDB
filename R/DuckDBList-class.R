#' DuckDBList objects
#'
#' @description
#' The DuckDBList virtual class extends the \linkS4class{List} virtual
#' class for DuckDB tables using a design inspirted by the the
#' \linkS4class{CompressedList} virtual class.
#'
#' @details
#' Similar to the \linkS4class{CompressedList} class, the DuckDBList virtual
#' class uses \code{unlistData} and \code{partitioning} slots that define the
#' unlisted data and how to partition the data into a list, respectively. The
#' difference is that the \code{partitioning} slot containing a logical
#' expression rather than a \linkS4class{PartitioningByEnd} object.
#'
#' @section Accessors:
#' In the code snippets below, \code{x} is a DuckDBList object:
#' \describe{
#'   \item{\code{length(x)}:}{
#'     Get the number of elements in \code{x}.
#'   }
#'   \item{\code{names(x)}, \code{names(x) <- value}:}{
#'     Get or set the names of the elements of \code{x}.
#'   }
#'   \item{\code{mcols(x)}, \code{mcols(x) <- value}:}{
#'      Get or set the metadata columns.
#'   }
#'   \item{\code{elementNROWS(x)}:}{
#'     Get the length (or nb of row for a matrix-like object) of each of the
#'     elements.
#'   }
#'   \item{\code{dimtbls(x)}, \code{dimtbls(x) <- value}:}{
#'     Get or set the list of dimension tables used to define partitions for
#'     efficient queries.
#'   }
#' }
#'
#' @section Coercion:
#' In the code snippets below, \code{x} is a DuckDBList object:
#' \describe{
#'   \item{\code{unlist(x)}:}{
#'     Returns the underlying unlisted data.
#'   }
#' }
#'
#' @section Subsetting:
#' In the code snippets below, \code{x} is a DuckDBList object:
#' \describe{
#'   \item{\code{x[i]}:}{
#'     Returns a DuckDBList object containing the selected elements.
#'   }
#'   \item{\code{x[[i]]}:}{
#'     Return the selected list element \code{i}, where \code{i} is an numeric
#'     or character vector of length 1.
#'   }
#'   \item{\code{x$name}:}{
#'     Similar to \code{x[[name]]}, but \code{name} is taken literally as an
#'     element name.
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
#' DuckDBList-class
#'
#' dbconn,DuckDBList-method
#' tblconn,DuckDBList-method
#' dimtbls,DuckDBList-method
#' dimtbls<-,DuckDBList-method
#' length,DuckDBList-method
#' names,DuckDBList-method
#' names<-,DuckDBList-method
#' elementNROWS,DuckDBList-method
#'
#' unlist,DuckDBList-method
#'
#' extractROWS,DuckDBList,ANY-method
#' getListElement,DuckDBList-method
#' head,DuckDBList-method
#' tail,DuckDBList-method
#'
#' @include DuckDBTable-class.R
#'
#' @keywords classes methods
#'
#' @name DuckDBList-class
NULL

#' @export
#' @importClassesFrom S4Vectors List
#' @importFrom stats setNames
setClass("DuckDBList", contains = c("List", "VIRTUAL"),
         slots = c(unlistData = "ANY", partitioning = "expression",
                   names = "character", elementNROWS = "integer"),
         prototype = prototype(elementNROWS = setNames(integer(), character())))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

#' @export
#' @importFrom BiocGenerics dbconn
setMethod("dbconn", "DuckDBList", function(x) callGeneric(x@unlistData))

#' @export
setMethod("tblconn", "DuckDBList", function(x, select = TRUE, filter = TRUE) {
    callGeneric(x@unlistData, select = select, filter = filter)
})

#' @export
setMethod("dimtbls", "DuckDBList", function(x) callGeneric(x@unlistData))

#' @export
setReplaceMethod("dimtbls", "DuckDBList", function(x, value) {
    callGeneric(x@unlistData, value)
})

#' @export
setMethod("length", "DuckDBList", function(x) length(x@names))

#' @export
setMethod("names", "DuckDBList", function(x) names(x@names) %||% x@names)

#' @export
#' @importFrom S4Vectors mcols
setReplaceMethod("names", "DuckDBList", function(x, value) {
    x_names <- x@names
    names(x_names) <- value
    mc <- mcols(x)
    if (!is.null(mc)) {
        rownames(mc) <- value
    }
    replaceSlots(x, names = x_names, elementMetadata = mc, check = FALSE)
})

#' @export
#' @importFrom S4Vectors elementNROWS
#' @importFrom stats setNames
setMethod("elementNROWS", "DuckDBList", function(x) setNames(x@elementNROWS, names(x)))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Validity
###

#' @importFrom S4Vectors setValidity2
setValidity2("DuckDBList", function(x) {
    msg <- NULL
    if (NROW(x) > 0L) {
        if (length(x@partitioning) != 1L) {
            msg <- c(msg, "must have exactly one partitioning expression")
        }
    }
    if (length(x@names) != length(x@elementNROWS)) {
        msg <- c(msg, "'names' and 'elementNROWS' must have the same length")
    }
    if (!identical(unname(x@names), names(x@elementNROWS))) {
        msg <- c(msg, "'names' and 'elementNROWS' must use the same names")
    }
    if (is.null(names(x@elementNROWS))) {
        msg <- c(msg, "'elementNROWS' must be a named integer64 vector")
    }
    msg %||% TRUE
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Unlisting
###

#' @export
#' @importFrom BiocGenerics unlist
#' @importFrom S4Vectors new2
setMethod("unlist", "DuckDBList", function(x, recursive = TRUE, use.names = TRUE) {
    unlistData <- x@unlistData
    conn <- tblconn(unlistData, select = FALSE, filter = FALSE)
    datacols <- x@partitioning
    keycols <- .keycols(unlistData)
    dimtbls <- dimtbls(unlistData)
    table <- new2("DuckDBTable", conn = conn, datacols = datacols, keycols = keycols,
                  dimtbls = dimtbls, check = FALSE)
    group <- new2("DuckDBColumn", table = table, check = FALSE)
    keep <- group %in% x@names
    extractROWS(unlistData, keep)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Subsetting
###

#' @export
#' @importFrom S4Vectors extractROWS normalizeSingleBracketSubscript
setMethod("extractROWS", "DuckDBList", function(x, i) {
    if (missing(i)) {
        return(x)
    }

    i <- normalizeSingleBracketSubscript(i, elementNROWS(x))

    names <- x@names[i]

    elementNROWS <- x@elementNROWS[i]

    mcols <- x@elementMetadata
    if (NROW(mcols) > 0L) {
        mcols <- callGeneric(mcols, i = i)
    }

    replaceSlots(x, names = names, elementNROWS = elementNROWS, elementMetadata = mcols, check = FALSE)
})

#' @export
#' @importFrom S4Vectors getListElement new2 normalizeDoubleBracketSubscript
setMethod("getListElement", "DuckDBList", function(x, i) {
    i <- normalizeDoubleBracketSubscript(i, elementNROWS(x))
    unlistData <- x@unlistData
    conn <- tblconn(unlistData, select = FALSE, filter = FALSE)
    datacols <- x@partitioning
    keycols <- .keycols(unlistData)
    dimtbls <- dimtbls(unlistData)
    table <- new2("DuckDBTable", conn = conn, datacols = datacols, keycols = keycols,
                  dimtbls = dimtbls, check = FALSE)
    group <- new2("DuckDBColumn", table = table, check = FALSE)
    keep <- group == x@names[i]
    extractROWS(unlistData, keep)
})

#' @export
#' @importFrom S4Vectors head
setMethod("head", "DuckDBList", function(x, n = 6L, ...) {
    names <- callGeneric(x@names, n = n, ...)

    elementNROWS <- callGeneric(x@elementNROWS, n = n, ...)

    mcols <- x@elementMetadata
    if (!is.null(mcols)) {
        mcols <- callGeneric(mcols, n = n, ...)
    }

    replaceSlots(x, names = names, elementNROWS = elementNROWS, elementMetadata = mcols, check = FALSE)
})

#' @export
#' @importFrom S4Vectors tail
setMethod("tail", "DuckDBList", function(x, n = 6L, ...) {
    names <- callGeneric(x@names, n = n, ...)

    elementNROWS <- callGeneric(x@elementNROWS, n = n, ...)

    mcols <- x@elementMetadata
    if (!is.null(mcols)) {
        mcols <- callGeneric(mcols, n = n, ...)
    }

    replaceSlots(x, names = names, elementNROWS = elementNROWS, elementMetadata = mcols, check = FALSE)
})
