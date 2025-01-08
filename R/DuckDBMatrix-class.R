#' DuckDBMatrix objects
#'
#' @description
#' The DuckDBMatrix class is a \link[DelayedArray]{DelayedMatrix} subclass
#' for representing and operating on a DuckDB table.
#'
#' All the operations available for \link[DelayedArray]{DelayedMatrix}
#' objects work on DuckDBMatrix objects.
#'
#' @section Constructor:
#' \describe{
#'   \item{\code{DuckDBMatrix(conn, datacol, row, col, keycols = c(row, col), dimtbls = NULL, type = NULL)}:}{
#'     Creates a DuckDBMatrix object.
#'     \describe{
#'       \item{\code{conn}}{
#'         Either a character vector containing the paths to parquet, csv, or
#'         gzipped csv data files; a string that defines a duckdb \code{read_*}
#'         data source; a DuckDBDataFrame object; or a tbl_duckdb_connection
#'         object.
#'       }
#'       \item{\code{datacol}}{
#'         Either a string specifying the column from \code{conn} or a named
#'         \code{expression} that will be evaluated in the context of
#'         \code{conn} that defines the values in the matrix.
#'       }
#'       \item{\code{row}}{
#'         Either a string that specifies the column in \code{conn} that
#'         specifies the row names of the matrix, or a named list containing a
#'         single character vector that defines the column in \code{conn} for
#'         the row names and its values.
#'       }
#'       \item{\code{col}}{
#'          Either a string that specifies the column in \code{conn} that
#'          specifies the column names of the matrix, or a named list containing
#'          a single character vector that defines the column in \code{conn} for
#'          the column names and its values.
#'       }
#'       \item{\code{keycols}}{
#'         An optional character vector that define the names of the columns in
#'         \code{conn} for the rows and columns of the matrix, or a named list
#'         of character vectors where the names of the list define rows and
#'         columns and the character vectors define distinct values for the rows
#'         and columns.
#'       }
#'       \item{\code{dimtbls}}{
#'         A optional named \code{DataFrameList} that specifies the dimension
#'         tables associated with the \code{keycols}. The name of the list
#'         elements match the names of the \code{keycols} list. Additionally,
#'         the \code{DataFrame} objects have row names that match the distinct
#'         values of the corresponding \code{keycols} list element and columns
#'         that define partitions in the data table for efficient querying.
#'       }
#'       \item{\code{type}}{
#'         String specifying the type of the data values; one of
#'         \code{"logical"}, \code{"integer"}, \code{"integer64"},
#'         \code{"double"}, or \code{"character"}. If \code{NULL}, it is
#'         determined by inspecting the data.
#'       }
#'     }
#'   }
#' }
#'
#' @section Accessors:
#' In the code snippets below, \code{x} is a DuckDBMatrix object:
#' \describe{
#'   \item{\code{dim(x)}:}{
#'     An integer vector of the array dimensions.
#'   }
#'   \item{\code{dimnames(x)}:}{
#'     List of array dimension names.
#'   }
#'   \item{\code{dimtbls(x)}, \code{dimtbls(x) <- value}:}{
#'     Get or set the list of dimension tables used to define partitions for
#'     efficient queries.
#'   }
#'   \item{\code{type(x)}, \code{type(x) <- value}:}{
#'     Get or set the data type of the array elements; one of \code{"logical"},
#'     \code{"integer"}, \code{"integer64"}, \code{"double"}, or
#'     \code{"character"}.
#'   }
#' }
#'
#' @section Subsetting:
#' In the code snippets below, \code{x} is a DuckDBMatrix object:
#' \describe{
#'   \item{\code{x[i, j, ..., drop = TRUE]}:}{
#'     Returns a new DuckDBMatrix object. Empty dimensions are dropped if
#'     \code{drop = TRUE}.
#'   }
#' }
#'
#' @section Transposition:
#' In the code snippets below, \code{x} is a DuckDBMatrix object:
#' \describe{
#'   \item{\code{aperm(a, perm)}:}{
#'     Returns a new DuckDBMatrix object with the dimensions permuted
#'     according to the \code{perm} vector.
#'   }
#'   \item{\code{t(x)}:}{
#'     Returns a new DuckDBMatrix object with the rows and columns transposed.
#'   }
#' }
#'
#' @author Patrick Aboyoun
#'
#' @examples
#' # Create a data.frame from a matrix
#' df <- data.frame(
#'   rowname = rep(rownames(state.x77), times = ncol(state.x77)),
#'   colname = rep(colnames(state.x77), each = nrow(state.x77)),
#'   value = as.vector(state.x77)
#' )
#'
#' # Write data to a parquet file
#' tf <- tempfile(fileext = ".parquet")
#' on.exit(unlink(tf))
#' arrow::write_parquet(df, tf)
#'
#' pqmat <- DuckDBMatrix(tf, row = "rowname", col = "colname", datacol = "value")
#'
#' @aliases
#' DuckDBMatrix-class
#' matrixClass,DuckDBArray-method
#'
#' DuckDBMatrix
#'
#' [,DuckDBMatrix,ANY,ANY,ANY-method
#'
#' @include DuckDBArray-class.R
#'
#' @keywords classes methods
#'
#' @name DuckDBMatrix-class
NULL

#' @export
#' @importClassesFrom DelayedArray DelayedMatrix
setClass("DuckDBMatrix", contains = c("DuckDBArray", "DelayedMatrix"))

#' @export
#' @importFrom DelayedArray matrixClass
setMethod("matrixClass", "DuckDBArray", function(x) "DuckDBMatrix")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Validity
###

#' @importFrom S4Vectors setValidity2
setValidity2("DuckDBMatrix", function(x) {
    msg <- NULL
    if (nkey(x@seed@table) != 2L) {
        msg <- c(msg, "'keycols' seed slot must be a two element named list of character vectors")
    }
    msg %||% TRUE
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

#' @export
#' @importFrom S4Vectors isSingleString new2
#' @importFrom stats setNames
DuckDBMatrix <-
function(conn, datacol, row, col, keycols = c(row, col), dimtbls = NULL, type = NULL) {
    if (!missing(row) && isSingleString(row)) {
        row <- setNames(list(NULL), row)
    }
    if (!missing(col) && isSingleString(col)) {
        col <- setNames(list(NULL), col)
    }
    if (!is(conn, "DuckDBArraySeed")) {
        if (length(keycols) != 2L) {
            stop("'keycols' must contain exactly 2 elements: rows and columns")
        }
        conn <- DuckDBArraySeed(conn, datacol = datacol, keycols = keycols,
                                dimtbls = dimtbls, type = type)
    }
    new2("DuckDBMatrix", seed = conn, check = FALSE)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Subsetting
###

#' @export
setMethod("[", "DuckDBMatrix", function(x, i, j, ..., drop = TRUE) {
    Nindex <- S4Arrays:::extract_Nindex_from_syscall(sys.call(), parent.frame())
    seed <- .subset_DuckDBArraySeed(x@seed, Nindex = Nindex, drop = drop)
    if (length(dim(seed)) == 1L) {
        DuckDBArray(seed)
    } else {
        replaceSlots(x, seed = seed, check = FALSE)
    }
})
