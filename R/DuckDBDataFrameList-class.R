#' DuckDB-backed DataFrameList
#'
#' @description
#' Create a DuckDB-backed \linkS4class{DataFrameList} object.
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
#' @include DuckDBDataFrame-class.R
#' @include DuckDBList-class.R
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

# ncols method inherited from DataFrameList

#' @export
#' @importFrom BiocGenerics ncols
setMethod("ncols", "DuckDBDataFrameList", getMethod("ncols", "SimpleSplitDataFrameList"))

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
setMethod("commonColnames", "DuckDBDataFrameList", getMethod("commonColnames", "SimpleSplitDataFrameList"))

# commonColnames<- method inherited from SplitDataFrameList

#' @export
#' @importFrom IRanges columnMetadata
setMethod("columnMetadata", "DuckDBDataFrameList", getMethod("columnMetadata", "SimpleSplitDataFrameList"))

#' @export
#' @importFrom IRanges columnMetadata<-
setReplaceMethod("columnMetadata", "DuckDBDataFrameList", function(x, value) {
    if (length(x) != length(value)) {
        stop("replacement value must be the same length as x")
    }
    mcols(x@unlistData) <- value[[1L]]
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
