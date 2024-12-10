#' DuckDBGRangesList objects
#'
#' @description
#' The DuckDBDataFrame class extends both \linkS4class{GRangesList} and
#' \linkS4class{DuckDBList} to represent a DuckDB table as a
#' \linkS4class{GRangesList} object.
#'
#' @details
#' The DuckDBGRangesList class extends the \linkS4class{GRangesList} instead of
#' \linkS4class{GenomicRangesList} class because the \code{rowRanges} slot
#' accepts either a \linkS4class{GenomicRanges} object or a
#' \linkS4class{GRangesList} object. As a result, it is necessary to override
#' certain methods that were inherited from \linkS4class{GRangesList} that would
#' have ideally been inherited from the \linkS4class{DuckDBList} class.
#'
#' @section Constructor:
#' \describe{
#'   \item{\code{split(x, f)}:}{
#'     Creates a DuckDBGRangesList object.
#'     \describe{
#'       \item{\code{x}}{
#'         A DuckDBGRanges object to split.
#'       }
#'       \item{\code{f}}{
#'         A DuckDBColumn object to split \code{x} by.
#'       }
#'     }
#'   }
#' }
#'
#' @section Accessors:
#' In the code snippets below, \code{x} is a DuckDBGRangesList object:
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
#' }
#'
#' @section Coercion:
#' In the code snippets below, \code{x} is a DuckDBGRangesList object:
#' \describe{
#'   \item{\code{unlist(x)}:}{
#'     Returns the underlying DuckDBGRanges object.
#'   }
#' }
#'
#' @section Subsetting:
#' In the code snippets below, \code{x} is a DuckDBGRangesList object:
#' \describe{
#'   \item{\code{x[i]}:}{
#'     Returns a DuckDBGRangesList object containing the selected elements.
#'   }
#'   \item{\code{x[[i]]}:}{
#'     Return the selected DuckDBGRanges by \code{i}, where \code{i} is an
#'     numeric or character vector of length 1.
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
#' DuckDBGRangesList-class
#'
#' updateObject,DuckDBGRangesList-method
#'
#' length,DuckDBGRangesList-method
#' names,DuckDBGRangesList-method
#' names<-,DuckDBGRangesList-method
#' elementNROWS,DuckDBGRangesList-method
#'
#' split,DuckDBGRanges,DuckDBColumn-method
#'
#' unlist,DuckDBGRangesList-method
#'
#' extractROWS,DuckDBGRangesList,ANY-method
#' getListElement,DuckDBGRangesList-method
#' head,DuckDBGRangesList-method
#' tail,DuckDBGRangesList-method
#'
#' coerce,DuckDBGRangesList,DuckDBDataFrameList-method
#' coerce,DuckDBGRangesList,CompressedGRangesList-method
#'
#' show,DuckDBGRangesList-method
#'
#' @include DuckDBGRanges-class.R
#' @include DuckDBList-class.R
#'
#' @keywords classes methods
#'
#' @name DuckDBGRangesList-class
NULL

#' @export
#' @importClassesFrom IRanges SplitDataFrameList
setClass("DuckDBGRangesList", contains = c("GRangesList", "DuckDBList"),
         prototype = prototype(elementType = "DuckDBGRanges", unlistData = new("DuckDBGRanges")))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Updating
###

#' @export
#' @importFrom BiocGenerics updateObject
setMethod("updateObject", "DuckDBGRangesList", function(object, ..., verbose = FALSE) {
    object
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

#' @export
setMethod("length", "DuckDBGRangesList", getMethod("length", "DuckDBList"))

#' @export
setMethod("names", "DuckDBGRangesList", getMethod("names", "DuckDBList"))

#' @export
setReplaceMethod("names", "DuckDBGRangesList", getMethod("names<-", "DuckDBList"))

#' @export
#' @importFrom S4Vectors elementNROWS
setMethod("elementNROWS", "DuckDBGRangesList", getMethod("elementNROWS", "DuckDBList"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Splitting
###

#' @export
#' @importFrom S4Vectors make_zero_col_DFrame new2 split
#' @importFrom stats setNames
setMethod("split", c("DuckDBGRanges", "DuckDBColumn"), function(x, f, drop = FALSE, ...) {
    if (!isTRUE(all.equal(as(x@frame, "DuckDBTable"), f@table))) {
        stop("cannot split a DuckDBGRanges object by an incompatible DuckDBColumn object")
    }
    elementNROWS <- table(f)
    elementNROWS <- setNames(as.vector(elementNROWS), names(elementNROWS))
    new2("DuckDBGRangesList", unlistData = x, partitioning = f@table@datacols,
         names = names(elementNROWS), elementNROWS = elementNROWS,
         elementMetadata = make_zero_col_DFrame(length(elementNROWS)),
         check = FALSE)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Unlisting
###

#' @export
#' @importFrom BiocGenerics unlist
setMethod("unlist", "DuckDBGRangesList",  getMethod("unlist", "DuckDBList"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Subsetting
###

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

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

#' @export
#' @importFrom S4Vectors new2
setAs("DuckDBGRangesList", "DuckDBDataFrameList", function(from) {
    gr <- from@unlistData
    df <- gr@frame
    if (ncol(gr@elementMetadata) > 0L) {
        df <- cbind.DuckDBDataFrame(df, gr@elementMetadata)
    }
    new2("DuckDBDataFrameList", unlistData = df, partitioning = from@partitioning,
         names = from@names, elementNROWS = from@elementNROWS, check = FALSE)
})

#' @export
#' @importClassesFrom GenomicRanges CompressedGRangesList
#' @importClassesFrom S4Vectors DFrame
#' @importFrom S4Vectors mcols mcols<- metadata metadata<- split
setAs("DuckDBGRangesList", "CompressedGRangesList", function(from) {
    unlistData <- unlist(as(from, "DuckDBDataFrameList"))
    datacols <- c(unlistData@datacols, from@partitioning)
    names(datacols) <- make.unique(names(datacols), sep = "_")
    unlistData <- replaceSlots(unlistData, datacols = datacols, check = FALSE)

    df <- as(unlistData, "DFrame")
    group <- df[[ncol(df)]]
    df <- df[-ncol(df)]

    seqnames <- Rle(df[["seqnames"]])
    ranges <- IRanges(start = df[["start"]], width = df[["width"]])
    if (!.has_row_number(from@unlistData)) {
        names(ranges) <- rownames(df)
    }
    strand <- strand(df[["strand"]])
    gr <- GRanges(seqnames, ranges = ranges, strand = strand,
                  seqinfo = seqinfo(from@unlistData))

    metadata(gr) <- metadata(from@unlistData)
    mc <- mcols(from@unlistData)
    if (ncol(mc) > 0L) {
        mcols(gr) <- as(df[, colnames(mc), drop = FALSE], "DFrame")
    }

    grlist <- split(gr, group)

    metadata(grlist) <- metadata(from)
    mc <- mcols(from)
    if (!is.null(mc)) {
        mcols(grlist) <- as(mc, "DFrame")
    }

    grlist
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Display
###

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
