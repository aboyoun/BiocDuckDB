#' DuckDB-backed List
#'
#' @description
#' Create a DuckDB-backed \linkS4class{List} object.
#'
#' @author Patrick Aboyoun
#'
#' @aliases
#' DuckDBList-class
#'
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
        if (length(x@expression) != 1L) {
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
    conn <- tblconn(unlistData)
    datacols <- x@partitioning
    keycols <- .keycols(unlistData)
    table <- new2("DuckDBTable", conn = conn, datacols = datacols, keycols = keycols, check = FALSE)
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
    conn <- tblconn(unlistData)
    datacols <- x@partitioning
    keycols <- .keycols(unlistData)
    table <- new2("DuckDBTable", conn = conn, datacols = datacols, keycols = keycols, check = FALSE)
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
