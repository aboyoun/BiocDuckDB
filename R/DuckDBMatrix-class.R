#' DuckDB tables as DelayedMatrix objects
#'
#' @description
#' The DuckDBMatrix class is a \link[DelayedArray]{DelayedMatrix} subclass
#' for representing and operating on a DuckDB table.
#'
#' All the operations available for \link[DelayedArray]{DelayedMatrix}
#' objects work on DuckDBMatrix objects.
#'
#' @param conn Either a character vector containing the paths to parquet, csv,
#' or gzipped csv data files; a string that defines a duckdb \code{read_*} data
#' source; a \code{DuckDBDataFrame} object; or a \code{tbl_duckdb_connection}
#' object.
#' @param datacols Either a string specifying the column from \code{conn} or a
#' named \code{expression} that will be evaluated in the context of \code{conn}
#' that defines the values in the matrix.
#' @param row Either a string that specifies the column in \code{conn} that
#' specifies the row names of the matrix, or a named list containing a single
#' character vector that defines the column in \code{conn} for the row names
#' and its values.
#' @param col Either a string that specifies the column in \code{conn} that
#' specifies the column names of the matrix, or a named list containing a single
#' character vector that defines the column in \code{conn} for the column names
#' and its values.
#' @param keycols An optional character vector that define the names of the
#' columns in \code{conn} for the rows and columns of the matrix, or a named
#' list of character vectors where the names of the list define rows and columns
#' and the character vectors define distinct values for the rows and columns.
#' @param type String specifying the type of the data values; one of
#' \code{"logical"}, \code{"integer"}, \code{"integer64"}, \code{"double"}, or
#' \code{"character"}. If \code{NULL}, it is determined by inspecting the data.
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
#' pqmat <- DuckDBMatrix(tf, row = "rowname", col = "colname", datacols = "value")
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
#' @rdname DuckDBMatrix-class
DuckDBMatrix <- function(conn, datacols, row, col, keycols = c(row, col), type = NULL) {
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
        conn <- DuckDBArraySeed(conn, datacols = datacols, keycols = keycols, type = type)
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
