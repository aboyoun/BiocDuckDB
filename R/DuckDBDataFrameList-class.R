#' DuckDBDataFrameList objects
#'
#' @description
#' The DuckDBDataFrame class extends both \linkS4class{SplitDataFrameList} and
#' \linkS4class{DuckDBList} to represent a DuckDB table as a
#' \linkS4class{SplitDataFrameList} object.
#'
#' @section Constructor:
#' \describe{
#'   \item{\code{split(x, f)}:}{
#'     Creates a DuckDBDataFrameList object.
#'     \describe{
#'       \item{\code{x}}{
#'         A DuckDBDataFrame object to split.
#'       }
#'       \item{\code{f}}{
#'         A DuckDBColumn object to split \code{x} by.
#'       }
#'     }
#'   }
#' }
#'
#' @section Accessors:
#' In the code snippets below, \code{x} is a DuckDBDataFrameList object:
#' \describe{
#'   \item{\code{dims(x)}:}{
#'     Get the two-column matrix indicating the number of rows and columns for
#'     each of the list elements.
#'   }
#'   \item{\code{nrows(x)}, \code{ncols(x)}:}{
#'     Get the number of rows and columns, respectively, for each of the list
#'     elements.
#'   }
#'   \item{\code{dimnames(x)}:}{
#'     Get the list of two \linkS4class{CharacterList}s, the first holding the
#'     rownames and the second the column names.
#'   }
#'   \item{\code{rownames(x)}, \code{colnames(x)}:}{
#'     Get the \linkS4class{CharacterList} of row and colum names, respectively.
#'   }
#'   \item{\code{commonColnames(x)}, \code{commonColnames(x) <- value}:}{
#'     Get or set the character vector of column names present in the individual
#'     DataFrames in \code{x}.
#'   }
#'   \item{\code{columnMetadata(x)}, \code{columnMetadata(x) <- value}:}{
#'     Get the \linkS4class{DataFrame} of metadata along the columns, i.e.,
#'     where each column in \code{x} is represented by a row in the metadata.
#'     The metadata is common across all elements of \code{x}. Note that calling
#'     \code{mcols(x)} returns the metadata on the \linkS4class{DataFrame}
#'     elements of \code{x}.
#'   }
#' }
#'
#' @section Coercion:
#' In the code snippets below, \code{x} is a DuckDBDataFrameList object:
#' \describe{
#'   \item{\code{unlist(x)}:}{
#'     Returns the underlying DuckDBDataFrame object.
#'   }
#'   \item{\code{as(from, "DFrameList")}:}{
#'     Converts a DuckDBDataFrameList object to a DFrameList object. This
#'     process involves first loading the data into memory using
#'     \code{as(from, "DFrame")}. The resulting DFrame is then split into a
#'     DFrameList. Additionally, any associated metadata and mcols (metadata
#'     columns) are preserved and added to the DFrameList, if they exist.
#'   }
#' }
#'
#' @section Subsetting:
#' In the code snippets below, \code{x} is a DuckDBDataFrameList object:
#' \describe{
#'   \item{\code{x[i]}:}{
#'     Returns a DuckDBDataFrameList object containing the selected elements.
#'   }
#'   \item{\code{x[[i]]}:}{
#'     Return the selected DuckDBDataFrame by \code{i}, where \code{i} is an
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
#' DuckDBDataFrameList-class
#'
#' ncols,DuckDBDataFrameList-method
#' rownames<-,DuckDBDataFrameList-method
#' colnames<-,DuckDBDataFrameList-method
#' commonColnames,DuckDBDataFrameList-method
#' columnMetadata,DuckDBDataFrameList-method
#' columnMetadata<-,DuckDBDataFrameList-method
#'
#' split,DuckDBDataFrame,DuckDBColumn-method
#'
#' coerce,DuckDBDataFrameList,DFrameList-method
#'
#' @include DuckDBDataFrame-class.R
#' @include DuckDBList-class.R
#'
#' @keywords classes methods
#'
#' @name DuckDBDataFrameList-class
NULL

#' @export
#' @importClassesFrom IRanges SplitDataFrameList
setClass("DuckDBDataFrameList", contains = c("SplitDataFrameList", "DuckDBList"),
         prototype = prototype(elementType = "DuckDBDataFrame", unlistData = new("DuckDBDataFrame")))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

# nrows method inherited from DataFrameList

#' @export
#' @importFrom BiocGenerics ncols
#' @importFrom S4Vectors isTRUEorFALSE
setMethod("ncols", "DuckDBDataFrameList", function(x, use.names = TRUE) {
    if (!isTRUEorFALSE(use.names)) {
        stop("'use.names' must be TRUE or FALSE")
    }
    ans <- rep.int(ncol(x@unlistData), length(x))
    if (use.names) {
        names(ans) <- names(x)
    }
    ans
})

# dims method inherited from DataFrameList
# rownames method inherited from DataFrameList
# colnames method inherited from SplitDataFrameList
# dimnames method inherited from DataFrameList

#' @export
#' @importFrom BiocGenerics rownames<-
setReplaceMethod("rownames", "DuckDBDataFrameList", function(x, value) {
    stop("cannot replace the rownames of a DuckDBDataFrameList object")
})

#' @export
#' @importClassesFrom IRanges CharacterList
#' @importFrom BiocGenerics colnames<-
setReplaceMethod("colnames", "DuckDBDataFrameList", function(x, value) {
    if (is.null(value) || is.character(value)) {
        colnames(x@unlistData) <- value
    } else if (is(value, "CharacterList")) {
        if (length(x) != length(value)) {
            stop("replacement value must be the same length as x")
        } else if (!all(vapply(value, function(y) identical(y, value[[1L]]), logical(1L)))) {
            stop("replacement value must be a CharacterList with identical elements")
        }
        colnames(x@unlistData) <- value[[1L]]
    } else {
        stop("replacement value must either be NULL or a CharacterList")
    }
    x
})

# dimnames<- method inherited from DataFrameList
# length method inherited from DuckDBList
# names method inherited from DuckDBList
# names<- method inherited from DuckDBList
# NROW method inherited from DataFrameList
# ROWNAMES method inherited from DataFrameList
# ROWNAMES<- method inherited from DataFrameList

#' @export
#' @importFrom IRanges commonColnames
setMethod("commonColnames", "DuckDBDataFrameList", function(x) colnames(x@unlistData))

# commonColnames<- method inherited from SplitDataFrameList

#' @export
#' @importFrom IRanges columnMetadata
#' @importFrom S4Vectors mcols
setMethod("columnMetadata", "DuckDBDataFrameList", function(x) mcols(x@unlistData, use.names = FALSE))

#' @export
#' @importFrom IRanges columnMetadata<-
#' @importFrom S4Vectors mcols<-
setReplaceMethod("columnMetadata", "DuckDBDataFrameList", function(x, value) {
    mcols(x@unlistData) <- value
    x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Splitting
###

#' @export
#' @importFrom S4Vectors split
#' @importFrom stats setNames
setMethod("split", c("DuckDBDataFrame", "DuckDBColumn"), function(x, f, drop = FALSE, ...) {
    if (!isTRUE(all.equal(as(x, "DuckDBTable"), f@table))) {
        stop("cannot split a DuckDBDataFrame object by an incompatible DuckDBColumn object")
    }
    elementNROWS <- table(f)
    elementNROWS <- setNames(as.vector(elementNROWS), names(elementNROWS))
    new2("DuckDBDataFrameList", unlistData = x, partitioning = f@table@datacols,
         names = names(elementNROWS), elementNROWS = elementNROWS, check = FALSE)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

#' @export
#' @importClassesFrom IRanges DFrameList
#' @importClassesFrom S4Vectors DFrame
#' @importFrom S4Vectors mcols mcols<- metadata metadata<- split
setAs("DuckDBDataFrameList", "DFrameList", function(from) {
    unlistData <- unlist(from)
    datacols <- c(unlistData@datacols, from@partitioning)
    names(datacols) <- make.unique(names(datacols), sep = "_")
    unlistData <- replaceSlots(unlistData, datacols = datacols, check = FALSE)

    df <- as(unlistData, "DFrame")
    group <- df[[ncol(df)]]
    df <- df[-ncol(df)]
    dflist <- split(df, group)

    metadata(dflist) <- metadata(from)
    mc <- mcols(from)
    if (!is.null(mc)) {
        mcols(dflist) <- as(mc, "DFrame")
    }

    dflist
})
