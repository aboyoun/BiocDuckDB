#' DuckDB-backed GenomicRangesList
#'
#' @description
#' Create a DuckDB-backed \linkS4class{GenomicRangesList} object.
#'
#' @author Patrick Aboyoun
#'
#' @aliases
#' DuckDBGRangesList-class
#' show,DuckDBGRangesList-method
#' updateObject,DuckDBGRangesList-method
#' elementNROWS,DuckDBGRangesList-method
#' extractROWS,DuckDBGRangesList,ANY-method
#' getListElement,DuckDBGRangesList-method
#' head,DuckDBGRangesList-method
#' length,DuckDBGRangesList-method
#' names,DuckDBGRangesList-method
#' tail,DuckDBGRangesList-method
#' split,DuckDBGRanges,DuckDBColumn-method
#'
#' @include DuckDBGRanges.R
#' @include DuckDBList.R
#'
#' @name DuckDBGRangesList
NULL

#' @export
#' @importClassesFrom IRanges SplitDataFrameList
setClass("DuckDBGRangesList", contains = c("GRangesList", "DuckDBList"),
         prototype = prototype(elementType = "DuckDBGRanges", unlistData = new("DuckDBGRanges")))

#' @export
#' @importFrom S4Vectors classNameForDisplay elementNROWS
setMethod("show", "DuckDBGRangesList", function(object) {
    x_len <- length(object)
    cat(classNameForDisplay(object), " object of length ", x_len, ":\n",
        sep = "")
    cumsumN <- cumsum(elementNROWS(object))
    N <- tail(cumsumN, 1L)
    if (x_len == 0L) {
        cat("<0 elements>\n")
    } else if (x_len <= 3L || (x_len <= 5L && N <= 20L)) {
        ## Display full object.
        show(as.list(object))
    } else {
        ## Display truncated object.
        if (cumsumN[[3L]] <= 20L) {
            showK <- 3L
        } else if (cumsumN[[2L]] <= 20L) {
            showK <- 2L
        } else {
            showK <- 1L
        }
        show(as.list(object[seq_len(showK)]))
        diffK <- x_len - showK
        cat("...\n",
            "<", diffK, " more ", ngettext(diffK, "element", "elements"),
            ">\n", sep = "")
    }
})

#' @export
#' @importFrom BiocGenerics updateObject
setMethod("updateObject", "DuckDBGRangesList", function(object, ..., verbose = FALSE) {
    object
})

#' @export
setMethod("length", "DuckDBGRangesList", getMethod("length", "DuckDBList"))

#' @export
setMethod("names", "DuckDBGRangesList", getMethod("names", "DuckDBList"))

#' @export
#' @importFrom S4Vectors elementNROWS
setMethod("elementNROWS", "DuckDBGRangesList", getMethod("elementNROWS", "DuckDBList"))

#' @export
#' @importFrom S4Vectors extractROWS
setMethod("extractROWS", "DuckDBGRangesList", getMethod("extractROWS", c("DuckDBList", "ANY")))

#' @export
#' @importFrom S4Vectors getListElement
setMethod("getListElement", "DuckDBGRangesList", getMethod("getListElement", "DuckDBList"))

#' @export
#' @importFrom S4Vectors head
setMethod("head", "DuckDBGRangesList", getMethod("head", "DuckDBList"))

#' @export
#' @importFrom S4Vectors head
setMethod("tail", "DuckDBGRangesList", getMethod("tail", "DuckDBList"))

#' @export
#' @importFrom S4Vectors split
#' @importFrom stats setNames
setMethod("split", c("DuckDBGRanges", "DuckDBColumn"), function(x, f, drop = FALSE, ...) {
    if (!isTRUE(all.equal(as(x@frame, "DuckDBTable"), f@table))) {
        stop("cannot split a DuckDBGRanges object by an incompatible DuckDBColumn object")
    }
    elementNROWS <- table(f)
    elementNROWS <- setNames(as.vector(elementNROWS), names(elementNROWS))
    new2("DuckDBGRangesList", unlistData = x, partitioning = f@table@datacols,
         names = names(elementNROWS), elementNROWS = elementNROWS, check = FALSE)
})
