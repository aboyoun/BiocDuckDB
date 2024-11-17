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
#' @param conn Either a character vector containing the paths to parquet, csv,
#' or gzipped csv data files; a string that defines a duckdb \code{read_*} data
#' source; a \code{DuckDBDataFrame} object; or a \code{tbl_duckdb_connection}
#' object.
#' @param keycols Either a character vector of column names from \code{conn}
#' that will specify the dimension names, or a named list of character vectors
#' where the names of the list specify the dimension names and the character
#' vectors set the distinct values for the dimension names.
#' @param datacols Either a string specifying the column from \code{conn} or a
#' named \code{expression} that will be evaluated in the context of \code{conn}
#' that defines the values in the array.
#' @param type String specifying the type of the data values; one of
#' \code{"logical"}, \code{"integer"}, \code{"integer64"}, \code{"double"}, or
#' \code{"character"}. If \code{NULL}, it is determined by inspecting the data.
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
#' pqaseed <- DuckDBArraySeed(tf, keycols = c("Class", "Sex", "Age", "Survived"), datacols = "fate")
#'
#' @aliases
#' DuckDBArraySeed-class
#' show,DuckDBArraySeed-method
#' [,DuckDBArraySeed,ANY,ANY,ANY-method
#' aperm,DuckDBArraySeed-method
#' dbconn,DuckDBArraySeed-method
#' DelayedArray,DuckDBArraySeed-method
#' dim,DuckDBArraySeed-method
#' dimnames,DuckDBArraySeed-method
#' extract_array,DuckDBArraySeed-method
#' extract_sparse_array,DuckDBArraySeed-method
#' is_nonzero,DuckDBArraySeed-method
#' is_sparse,DuckDBArraySeed-method
#' is.finite,DuckDBArraySeed-method
#' is.infinite,DuckDBArraySeed-method
#' is.nan,DuckDBArraySeed-method
#' nzcount,DuckDBArraySeed-method
#' t,DuckDBArraySeed-method
#' tblconn,DuckDBArraySeed-method
#' type,DuckDBArraySeed-method
#' type<-,DuckDBArraySeed-method
#' Ops,DuckDBArraySeed,DuckDBArraySeed-method
#' Ops,DuckDBArraySeed,atomic-method
#' Ops,atomic,DuckDBArraySeed-method
#' Math,DuckDBArraySeed-method
#' Summary,DuckDBArraySeed-method
#' mean,DuckDBArraySeed-method
#' median.DuckDBArraySeed
#' quantile.DuckDBArraySeed
#' var,DuckDBArraySeed,ANY-method
#' sd,DuckDBArraySeed-method
#' mad,DuckDBArraySeed-method
#' IQR,DuckDBArraySeed-method
#'
#' @seealso
#' \code{\link{DuckDBArray}},
#' \code{\link[S4Arrays]{Array}}
#'
#' @include tblconn.R
#' @include DuckDBTable.R
#'
#' @name DuckDBArraySeed
NULL

#' @export
#' @import methods
#' @importClassesFrom S4Arrays Array
setClass("DuckDBArraySeed", contains = "Array",
         slots = c(table = "DuckDBTable", drop = "logical"),
         prototype = prototype(drop = FALSE))

#' @importFrom S4Vectors isTRUEorFALSE setValidity2 isSingleString
setValidity2("DuckDBArraySeed", function(x) {
    table <- x@table
    if (length(table@conn) > 0L) {
        if (ncol(table) != 1L) {
            return("'table' slot must be a single-column DuckDBTable")
        }
    }
    if (!isTRUEorFALSE(x@drop)) {
        return("'drop' slot must be TRUE or FALSE")
    }
    TRUE
})

#' @export
#' @importFrom S4Vectors classNameForDisplay
setMethod("show", "DuckDBArraySeed", function(object) {
    cat(sprintf("<%s>%s %s object of type \"%s\"\n",
                paste(dim(object), collapse = " x "),
                if (is_sparse(object)) " sparse" else "",
                classNameForDisplay(object), type(object)))
    invisible(NULL)
})

#' @export
#' @importFrom BiocGenerics dbconn
setMethod("dbconn", "DuckDBArraySeed", function(x) callGeneric(x@table))

#' @export
setMethod("tblconn", "DuckDBArraySeed", function(x) callGeneric(x@table))

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

#' @export
#' @importFrom BiocGenerics type
setMethod("type", "DuckDBArraySeed", function(x) {
    unname(coltypes(x@table))
})

#' @export
#' @importFrom BiocGenerics type<-
setReplaceMethod("type", "DuckDBArraySeed", function(x, value) {
    table <- x@table
    coltypes(table) <- value
    initialize2(x, table = table, check = FALSE)
})

#' @export
#' @importFrom SparseArray is_nonzero
setMethod("is_nonzero", "DuckDBArraySeed", function(x) {
    initialize2(x, table = callGeneric(x@table), check = FALSE)
})

#' @export
#' @importFrom SparseArray nzcount
setMethod("nzcount", "DuckDBArraySeed", function(x) {
    callGeneric(x@table)
})
#' @export
#' @importFrom S4Arrays is_sparse
setMethod("is_sparse", "DuckDBArraySeed", function(x) {
    callGeneric(x@table)
})

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

    initialize2(x, table = table[Nindex, ], drop = drop, check = FALSE)
}

#' @export
#' @importFrom S4Vectors isTRUEorFALSE
setMethod("[", "DuckDBArraySeed", function(x, i, j, ..., drop = TRUE) {
    Nindex <- S4Arrays:::extract_Nindex_from_syscall(sys.call(), parent.frame())
    .subset_DuckDBArraySeed(x, Nindex = Nindex, drop = drop)
})

#' @export
setMethod("Ops", c(e1 = "DuckDBArraySeed", e2 = "DuckDBArraySeed"), function(e1, e2) {
    if (!isTRUE(all.equal(e1@table, e2@table)) || !identical(e1@drop, e2@drop)) {
        stop("can only perform arithmetic operations with compatible objects")
    }
    initialize2(e1, table = callGeneric(e1@table, e2@table), check = FALSE)
})

#' @export
setMethod("Ops", c(e1 = "DuckDBArraySeed", e2 = "atomic"), function(e1, e2) {
    initialize2(e1, table = callGeneric(e1@table, e2), check = FALSE)
})

#' @export
setMethod("Ops", c(e1 = "atomic", e2 = "DuckDBArraySeed"), function(e1, e2) {
    initialize2(e2, table = callGeneric(e1, e2@table), check = FALSE)
})

#' @export
setMethod("Math", "DuckDBArraySeed", function(x) {
    initialize2(x, table = callGeneric(x@table), check = FALSE)
})

#' @export
setMethod("is.finite", "DuckDBArraySeed", function(x) {
    initialize2(x, table = is.finite(x@table), check = FALSE)
})

#' @export
setMethod("is.infinite", "DuckDBArraySeed", function(x) {
    initialize2(x, table = is.infinite(x@table), check = FALSE)
})

#' @export
setMethod("is.nan", "DuckDBArraySeed", function(x) {
    initialize2(x, table = is.nan(x@table), check = FALSE)
})

#' @export
setMethod("Summary", "DuckDBArraySeed", function(x, ..., na.rm = FALSE) {
    callGeneric(x@table)
})

#' @export
#' @importFrom BiocGenerics mean
setMethod("mean", "DuckDBArraySeed", function(x, ...) {
    callGeneric(x@table)
})

#' @exportS3Method stats::median
#' @importFrom stats median
median.DuckDBArraySeed <- function(x, na.rm = FALSE, ...) {
    median(x@table, na.rm = na.rm, ...)
}

#' @exportS3Method stats::quantile
#' @importFrom stats quantile
quantile.DuckDBArraySeed <-
function(x, probs = seq(0, 1, 0.25), na.rm = FALSE, names = TRUE, type = 7, digits = 7, ...) {
    quantile(x@table, probs = probs, na.rm = na.rm, names = names, type = type, digits = digits, ...)
}

#' @export
#' @importFrom BiocGenerics var
setMethod("var", "DuckDBArraySeed", function(x, y = NULL, na.rm = FALSE, use)  {
    callGeneric(x@table)
})

#' @export
#' @importFrom BiocGenerics sd
setMethod("sd", "DuckDBArraySeed", function(x, na.rm = FALSE) {
    callGeneric(x@table)
})

#' @export
#' @importFrom BiocGenerics mad
setMethod("mad", "DuckDBArraySeed",
function(x, center = median(x), constant = 1.4826, na.rm = FALSE, low = FALSE, high = FALSE) {
    callGeneric(x@table, constant = constant)
})

#' @export
#' @importFrom BiocGenerics IQR
setMethod("IQR", "DuckDBArraySeed", function(x, na.rm = FALSE, type = 7) {
    callGeneric(x@table, type = type)
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
    fill <- switch(type(x), logical = FALSE, integer = 0L, double = 0, character =, raw = "")
    output <- array(fill, dim = lengths(index, use.names = FALSE))
    if (min(dim(output)) == 0L) {
        return(output)
    }
    dimnames(output) <- index

    # Fill output array
    table <- x@table[index, ]
    df <- as.data.frame(table)
    keycols <- df[, keynames(table)]
    output[as.matrix(keycols)] <- df[[colnames(table)]]
    if (x@drop) {
        output <- as.array(drop(output))
    }

    output
})

#' @export
#' @importClassesFrom SparseArray SVT_SparseArray
#' @importFrom SparseArray COO_SparseArray extract_sparse_array
setMethod("extract_sparse_array", "DuckDBArraySeed", function(x, index) {
    index <- .extract_array_index(x, index)
    table <- x@table[index, ]
    df <- as.data.frame(table)

    dim <- dim(x)
    dimnames <- dimnames(x)
    nzcoo <- sapply(names(dimnames), function(j) match(df[[j]], dimnames[[j]]))
    nzdata <- df[[colnames(table)]]
    coo <- COO_SparseArray(dim = dim, nzcoo = nzcoo, nzdata = nzdata, dimnames = dimnames)
    as(coo, "SVT_SparseArray")
})

#' @export
#' @importFrom DelayedArray DelayedArray
setMethod("DelayedArray", "DuckDBArraySeed", function(seed) DuckDBArray(seed))

#' @export
#' @importFrom S4Vectors new2
#' @importFrom stats setNames
#' @rdname DuckDBArraySeed
DuckDBArraySeed <- function(conn, keycols, datacols, type = NULL) {
    if (!is.null(type)) {
        type <- setNames(type, names(datacols) %||% datacols)
    }
    table <- DuckDBTable(conn, keycols = keycols, datacols = datacols, type = type)
    new2("DuckDBArraySeed", table = table, drop = FALSE, check = FALSE)
}
