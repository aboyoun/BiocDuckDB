#' DuckDBTransposedDataFrame objects
#'
#' @description
#' The DuckDBTransposedDataFrame class extends \linkS4class{TransposedDataFrame}
#' to represent a DuckDB table as a \linkS4class{TransposedDataFrame} object.
#'
#' @details
#' DuckDBTransposedDataFrame objects are constructed by calling \code{t()} on a
#' \linkS4class{DuckDBDataFrame} object.
#'
#' @section Constructor:
#' \describe{
#'   \item{\code{t(x)}:}{
#'     Creates a DuckDBTransposedDataFrame object from a DuckDBDataFrame object.
#'   }
#' }
#'
#' @section Accessors:
#' In the code snippets below, \code{x} is a DuckDBTransposedDataFrame object:
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
#' @section Subsetting:
#' In the code snippets below, \code{x} is a DuckDBTransposedDataFrame object:
#' \describe{
#'   \item{\code{x[i, j, drop = TRUE]}:}{
#'     Returns either a new DuckDBTransposedDataFrame object or a DuckDBColumn
#'     if selecting a single row and \code{drop = TRUE}.
#'   }
#' }
#'
#' @section Displaying:
#' The \code{show()} method for DuckDBTransposedDataFrame objects obeys global
#' options \code{showHeadLines} and \code{showTailLines} for controlling the
#' number of head and tail rows to display.
#'
#' @author Patrick Aboyoun
#'
#' @examples
#' # Mocking up a file:
#' tf <- tempfile(fileext = ".parquet")
#' on.exit(unlink(tf))
#' arrow::write_parquet(cbind(model = rownames(mtcars), mtcars), tf)
#'
#' # Creating our DuckDB-backed data frame:
#' tdf <- t(DuckDBDataFrame(tf, datacols = colnames(mtcars), keycol = "model"))
#' tdf
#'
#' # Slicing DuckDBTransposedDataFrame objects:
#' tdf[,1:5]
#' tdf[1:5,]
#'
#' @aliases
#' DuckDBTransposedDataFrame-class
#'
#' dbconn,DuckDBTransposedDataFrame-method
#' tblconn,DuckDBTransposedDataFrame-method
#' dimtbls,DuckDBTransposedDataFrame-method
#' dimtbls<-,DuckDBTransposedDataFrame-method
#'
#' t,DuckDBDataFrame-method
#' t.DuckDBDataFrame
#'
#' as.matrix,DuckDBTransposedDataFrame-method
#' realize,DuckDBTransposedDataFrame-method
#'
#' show,DuckDBTransposedDataFrame-method
#'
#' @seealso
#' \itemize{
#'   \item \code{\link{DuckDBDataFrame-class}} for the DataFrame class
#'   \item \code{\link[S4Vectors]{TransposedDataFrame}} for the base class
#' }
#'
#' @include DuckDBDataFrame-class.R
#'
#' @keywords classes methods
#'
#' @name DuckDBTransposedDataFrame-class
NULL

#' @export
#' @importClassesFrom S4Vectors TransposedDataFrame
setClass("DuckDBTransposedDataFrame", contains = "TransposedDataFrame",
    slots = c(data = "DuckDBDataFrame"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

#' @export
#' @importFrom BiocGenerics dbconn
setMethod("dbconn", "DuckDBTransposedDataFrame", function(x) callGeneric(x@data))

#' @export
setMethod("tblconn", "DuckDBTransposedDataFrame", function(x, select = TRUE, filter = TRUE) {
    callGeneric(x@data, select = select, filter = filter)
})

#' @export
setMethod("dimtbls", "DuckDBTransposedDataFrame", function(x) callGeneric(x@data))

#' @export
setReplaceMethod("dimtbls", "DuckDBTransposedDataFrame", function(x, value) {
    callGeneric(x@data, value)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Transposition
###

#' @export
#' @importFrom S4Vectors mcols mcols<- new2
t.DuckDBDataFrame <- function(x)
{
    x_mcols <- mcols(x, use.names = FALSE)
    if (!is.null(x_mcols)) {
        mcols(x) <- NULL
    }
    new2("DuckDBTransposedDataFrame", data = x, elementMetadata = x_mcols,
         check = FALSE)
}

#' @export
#' @importFrom BiocGenerics t
setMethod("t", "DuckDBDataFrame", t.DuckDBDataFrame)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

#' @export
#' @importFrom bit64 is.integer64
setMethod("as.matrix", "DuckDBTransposedDataFrame", function(x) {
    df <- as.data.frame(x@data)
    for (j in seq_along(df)) {
        if (is.integer64(df[[j]])) {
            df[[j]] <- as.double(df[[j]])
        }
    }
    mat <- do.call(rbind, df)
    colnames(mat) <- rownames(df)
    mat
})

#' @export
#' @importFrom DelayedArray getAutoRealizationBackend realize
#' @importFrom S4Vectors mcols mcols<- metadata metadata<-
setMethod("realize", "DuckDBTransposedDataFrame",
function(x, BACKEND = getAutoRealizationBackend()) {
    tdf <- t(callGeneric(x@data, BACKEND = BACKEND))

    metadata(tdf) <- metadata(x)
    mc <- mcols(x)
    if (!is.null(mc)) {
        mcols(tdf) <- as(mc, "DFrame")
    }

    tdf
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Display
###

#' @export
#' @importFrom S4Vectors classNameForDisplay
setMethod("show", "DuckDBTransposedDataFrame", function(object) {
    x_nrow <- nrow(object)
    x_ncol <- as.double(ncol(object))

    cat(classNameForDisplay(object), " with ",
        x_nrow, " row", ifelse(x_nrow == 1L, "", "s"), " and ",
        x_ncol, " column", ifelse(x_ncol == 1L, "", "s"), "\n", sep = "")

    if (x_nrow != 0L && x_ncol != 0L) {
        m <- t(.makePrettyCharacterMatrixForDisplay(object@data))
        print(m, quote = FALSE, right = TRUE)
    }

    invisible(NULL)
})
