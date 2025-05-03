#' DuckDBArraySeed objects
#'
#' @description
#' DuckDBArraySeed is a low-level helper class for representing a
#' pointer to a DuckDB table.
#'
#' Note that a DuckDBArraySeed object is not intended to be used directly.
#' Most end users will typically create and manipulate a higher-level
#' \link{DuckDBArray} object instead. See \code{?\link{DuckDBArray}} for
#' more information.
#'
#' @section Constructor:
#' \describe{
#'   \item{\code{DuckDBArraySeed(conn, datacol, keycols, dimtbls = NULL, type = NULL)}:}{
#'     Creates a DuckDBArraySeed object.
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
#'         \code{conn} that defines the values in the array.
#'       }
#'       \item{\code{keycols}}{
#'         Either a character vector of column names from \code{conn} that will
#'         specify the dimension names, or a named list of character vectors
#'         where the names of the list specify the dimension names and the
#'         character vectors set the distinct values for the dimension names.
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
#' In the code snippets below, \code{x} is a DuckDBArraySeed object:
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
#' In the code snippets below, \code{x} is a DuckDBArraySeed object:
#' \describe{
#'   \item{\code{x[i, j, ..., drop = TRUE]}:}{
#'     Returns a new DuckDBArraySeed object. Empty dimensions are dropped if
#'     \code{drop = TRUE}.
#'   }
#' }
#'
#' @section Transposition:
#' In the code snippets below, \code{x} is a DuckDBArraySeed object:
#' \describe{
#'   \item{\code{aperm(a, perm)}:}{
#'     Returns a new DuckDBArraySeed object with the dimensions permuted
#'     according to the \code{perm} vector.
#'   }
#'   \item{\code{t(x)}:}{
#'     For two-dimensional arrays, returns a new DuckDBArraySeed object with the
#'     dimensions transposed.
#'   }
#' }
#'
#' @author Patrick Aboyoun
#'
#' @examples
#' # Create a data.frame from the Titanic data
#' df <- do.call(expand.grid, c(dimnames(Titanic), stringsAsFactors = FALSE))
#' df$fate <- as.integer(Titanic[as.matrix(df)])
#'
#' # Write data to a parquet file
#' tf <- tempfile(fileext = ".parquet")
#' on.exit(unlink(tf))
#' arrow::write_parquet(df, tf)
#'
#' pqaseed <- DuckDBArraySeed(tf, datacol = "fate", keycols = c("Class", "Sex", "Age", "Survived"))
#'
#' @aliases
#' DuckDBArraySeed-class
#'
#' dbconn,DuckDBArraySeed-method
#' tblconn,DuckDBArraySeed-method
#' dimtbls,DuckDBArraySeed-method
#' dimtbls<-,DuckDBArraySeed-method
#' type,DuckDBArraySeed-method
#' type<-,DuckDBArraySeed-method
#'
#' dim,DuckDBArraySeed-method
#' dimnames,DuckDBArraySeed-method
#' extract_array,DuckDBArraySeed-method
#' extract_sparse_array,DuckDBArraySeed-method
#' DelayedArray,DuckDBArraySeed-method
#'
#' DuckDBArraySeed
#'
#' [,DuckDBArraySeed,ANY,ANY,ANY-method
#'
#' aperm,DuckDBArraySeed-method
#' t,DuckDBArraySeed-method
#'
#' show,DuckDBArraySeed-method
#'
#' @seealso
#' \itemize{
#'   \item \code{\link{DuckDBArraySeed-utils}} for the utilities
#'   \item \code{\link{DuckDBArray-class}} for the main class
#'   \item \code{\link{DuckDBArray-utils}} for the main class utilities
#'   \item \code{\link[S4Arrays]{Array}} for the base class
#' }
#'
#' @include DuckDBTable-class.R
#'
#' @keywords classes methods
#'
#' @name DuckDBArraySeed-class
NULL

#' @export
#' @import methods
#' @importClassesFrom S4Arrays Array
setClass("DuckDBArraySeed", contains = "Array",
         slots = c(table = "DuckDBTable", fill = "atomic", drop = "logical"),
         prototype = prototype(fill = FALSE, drop = FALSE))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

#' @export
#' @importFrom BiocGenerics dbconn
setMethod("dbconn", "DuckDBArraySeed", function(x) callGeneric(x@table))

#' @export
setMethod("tblconn", "DuckDBArraySeed", function(x, select = TRUE, filter = TRUE) {
    callGeneric(x@table, select = select, filter = filter)
})

#' @export
setMethod("dimtbls", "DuckDBArraySeed", function(x) callGeneric(x@table))

#' @export
setReplaceMethod("dimtbls", "DuckDBArraySeed", function(x, value) {
    callGeneric(x@table, value)
})

#' @export
#' @importFrom BiocGenerics type
setMethod("type", "DuckDBArraySeed", function(x) unname(coltypes(x@table)))

#' @export
#' @importFrom BiocGenerics type<-
setReplaceMethod("type", "DuckDBArraySeed", function(x, value) {
    table <- x@table
    coltypes(table) <- value
    replaceSlots(x, table = table, check = FALSE)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Validity
###

#' @importFrom S4Vectors isTRUEorFALSE setValidity2 isSingleString
setValidity2("DuckDBArraySeed", function(x) {
    msg <- NULL
    table <- x@table
    if (length(table@conn) > 0L) {
        if (ncol(table) != 1L) {
            msg <- c(msg, "'table' slot must be a single-column DuckDBTable")
        }
    }
    if (length(x@fill) != 1L) {
        msg <- c(msg, "'fill' slot must be a single atomic value")
    }
    if (!isTRUEorFALSE(x@drop)) {
        msg <- c(msg, "'drop' slot must be TRUE or FALSE")
    }
    msg %||% TRUE
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Seed contract
###

#' @export
setMethod("dim", "DuckDBArraySeed", function(x) {
    ans <- nkeydim(x@table)
    if (x@drop) {
        keep <- ans != 1L
        if (!any(keep)) {
            ans <- 1L
        } else {
            ans <- ans[keep]
        }
    }
    ans
})

#' @export
setMethod("dimnames", "DuckDBArraySeed", function(x) {
    ans <- keydimnames(x@table)
    if (x@drop) {
        keep <- lengths(ans, use.names = FALSE) != 1L
        if (!any(keep)) {
            ans <- NULL
        } else {
            ans <- ans[keep]
        }
    }
    ans
})

.extract_array_index <- function(x, index) {
    if (!is.list(index)) {
        stop("'index' must be a list")
    }

    table <- x@table
    if (all(vapply(index, is.null, logical(1L)))) {
        index <- keydimnames(table)
    } else {
        # Add names to index list
        if (length(index) == nkey(table)) {
            names(index) <- keynames(table)
        } else if (length(index) == length(dimnames(x))) {
            names(index) <- names(dimnames(x))
        }
        # Replace NULL and integer values with strings
        for (i in names(index)) {
            idx <- index[[i]]
            if (is.null(idx)) {
                index[[i]] <- table@keycols[[i]]
            } else {
                index[[i]] <- table@keycols[[i]][idx]
            }
        }
        # Add dropped dimensions if data are present
        if ((length(index) < nkey(table)) && all(lengths(index, use.names = FALSE) > 0L)) {
            keycols <- keydimnames(table)
            keycols[names(index)] <- index
            index <- keycols
        }
    }

    index
}

#' @export
#' @importFrom S4Arrays extract_array
setMethod("extract_array", "DuckDBArraySeed", function(x, index) {
    index <- .extract_array_index(x, index)

    # Initialize output array
    output <- array(x@fill, dim = lengths(index, use.names = FALSE))
    if (min(dim(output)) == 0L) {
        return(output)
    }
    dimnames(output) <- index

    # Fill output array
    table <- x@table[index, ]
    df <- as.data.frame(table)
    keycols <- df[, keynames(table), drop = FALSE]
    midx <- do.call(cbind, lapply(keycols, as.character))
    output[midx] <- df[[colnames(table)]]
    if (x@drop) {
        output <- as.array(drop(output))
    }

    output
})

#' @export
#' @importFrom SparseArray COO_SparseArray extract_sparse_array
setMethod("extract_sparse_array", "DuckDBArraySeed", function(x, index) {
    if (!identical(coltypes(x@table), x@fill)) {
        return(as(extract_array(x, index), "COO_SparseArray"))
    }

    index <- .extract_array_index(x, index)
    table <- x@table[index, ]
    df <- as.data.frame(table)

    dim <- dim(x)
    dimnames <- dimnames(x)
    nzcoo <- lapply(names(dimnames), function(j) match(df[[j]], dimnames[[j]]))
    names(nzcoo) <- names(dimnames)
    nzcoo <- do.call(cbind, nzcoo)
    nzdata <- df[[colnames(table)]]
    COO_SparseArray(dim = dim, nzcoo = nzcoo, nzdata = nzdata,
                    dimnames = dimnames, check = FALSE)
})

#' @export
#' @importFrom DelayedArray DelayedArray
setMethod("DelayedArray", "DuckDBArraySeed", function(seed) DuckDBArray(seed))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

#' @export
#' @importFrom S4Vectors new2
#' @importFrom stats setNames
DuckDBArraySeed <- function(conn, datacol, keycols, dimtbls = NULL, type = NULL) {
    if (!is.null(type)) {
        type <- setNames(type, names(datacol) %||% datacol)
    }
    table <- DuckDBTable(conn, datacols = datacol, keycols = keycols,
                         dimtbls = dimtbls, type = type)
    fill <- vector(coltypes(table), 1L)
    new2("DuckDBArraySeed", table = table, fill = fill, drop = FALSE, check = FALSE)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Subsetting
###

.subset_DuckDBArraySeed <- function(x, Nindex, drop) {
    table <- x@table
    ndim <- nkey(table)
    nsubscript <- length(Nindex)
    if (nsubscript == 0L)
        return(x)  # no-op
    if (nsubscript != ndim) {
        stop("incorrect number of subscripts")
    }

    names(Nindex) <- keynames(table)
    for (i in names(Nindex)) {
        if (is.null(Nindex[[i]])) {
            Nindex[[i]] <- keydimnames(table)[[i]]
        }
    }

    replaceSlots(x, table = table[Nindex, ], drop = drop, check = FALSE)
}

#' @export
#' @importFrom S4Vectors isTRUEorFALSE
setMethod("[", "DuckDBArraySeed", function(x, i, j, ..., drop = TRUE) {
    Nindex <- S4Arrays:::extract_Nindex_from_syscall(sys.call(), parent.frame())
    .subset_DuckDBArraySeed(x, Nindex = Nindex, drop = drop)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Transposition
###

#' @export
#' @importFrom BiocGenerics aperm
setMethod("aperm", "DuckDBArraySeed", function(a, perm, ...) {
    k <- nkey(a@table)
    if ((length(perm) != k) || !setequal(perm, seq_len(k))) {
        stop("'perm' must be a permutation of 1:", k)
    }
    a@table@keycols <- a@table@keycols[perm]
    a
})

#' @export
#' @importFrom BiocGenerics t
setMethod("t", "DuckDBArraySeed", function(x) {
    if (nkey(x@table) != 2L) {
        stop("'t()' is only defined for 2-dimensional DuckDBArray objects")
    }
    aperm(x, perm = 2:1)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Display
###

#' @export
#' @importFrom S4Vectors classNameForDisplay
setMethod("show", "DuckDBArraySeed", function(object) {
    cat(sprintf("<%s>%s %s object of type \"%s\"\n",
                paste(dim(object), collapse = " x "),
                if (is_sparse(object)) " sparse" else "",
                classNameForDisplay(object), type(object)))
    invisible(NULL)
})
