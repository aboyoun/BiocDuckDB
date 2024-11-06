#' ParquetArraySeed objects
#'
#' @description
#' ParquetArraySeed is a low-level helper class for representing a
#' pointer to a Parquet dataset.
#'
#' Note that a ParquetArraySeed object is not intended to be used directly.
#' Most end users will typically create and manipulate a higher-level
#' \link{ParquetArray} object instead. See \code{?\link{ParquetArray}} for
#' more information.
#'
#' @param conn Either a string containing the path to the Parquet data or a
#' \code{tbl_duckdb_connection} object.
#' @param key Either a character vector or a list of character vectors
#' containing the names of the columns in the Parquet data that specify
#' the primary key of the array.
#' @param fact String containing the name of the column in the Parquet data
#' that specifies the value of the array.
#' @param type String specifying the type of the Parquet data values;
#' one of \code{"logical"}, \code{"integer"}, \code{"double"}, or
#' \code{"character"}. If \code{NULL}, this is determined by inspecting
#' the data.
#' @param ... Further arguments to be passed to \code{read_parquet}.
#'
#' @author Patrick Aboyoun
#'
#' @examples
#' # Create a data.frame from the Titanic data
#' df <- do.call(expand.grid, c(dimnames(Titanic), stringsAsFactors = FALSE))
#' df$fate <- as.integer(Titanic[as.matrix(df)])
#'
#' # Write data to a parquet file
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' arrow::write_parquet(df, tf)
#'
#' pqaseed <- ParquetArraySeed(tf, key = c("Class", "Sex", "Age", "Survived"), fact = "fate")
#'
#' @aliases
#' ParquetArraySeed-class
#' [,ParquetArraySeed,ANY,ANY,ANY-method
#' aperm,ParquetArraySeed-method
#' dbconn,ParquetArraySeed-method
#' DelayedArray,ParquetArraySeed-method
#' dim,ParquetArraySeed-method
#' dimnames,ParquetArraySeed-method
#' extract_array,ParquetArraySeed-method
#' extract_sparse_array,ParquetArraySeed-method
#' is_nonzero,ParquetArraySeed-method
#' is_sparse,ParquetArraySeed-method
#' nzcount,ParquetArraySeed-method
#' t,ParquetArraySeed-method
#' type,ParquetArraySeed-method
#' type<-,ParquetArraySeed-method
#' Ops,ParquetArraySeed,ParquetArraySeed-method
#' Ops,ParquetArraySeed,atomic-method
#' Ops,atomic,ParquetArraySeed-method
#' Math,ParquetArraySeed-method
#' Summary,ParquetArraySeed-method
#' mean,ParquetArraySeed-method
#' median.ParquetArraySeed
#' quantile.ParquetArraySeed
#' var,ParquetArraySeed,ANY-method
#' sd,ParquetArraySeed-method
#' mad,ParquetArraySeed-method
#'
#' @seealso
#' \code{\link{ParquetArray}},
#' \code{\link[S4Arrays]{Array}}
#'
#' @include acquireTable.R
#' @include ParquetFactTable.R
#'
#' @name ParquetArraySeed
NULL

#' @export
#' @import methods
#' @importClassesFrom S4Arrays Array
setClass("ParquetArraySeed", contains = "Array", slots = c(table = "ParquetFactTable", drop = "logical"))

#' @importFrom S4Vectors isTRUEorFALSE setValidity2 isSingleString
setValidity2("ParquetArraySeed", function(x) {
    if (ncol(x@table) != 1L) {
        return("'table' slot must be a single-column ParquetFactTable")
    }
    if (!isTRUEorFALSE(x@drop)) {
        return("'drop' slot must be TRUE or FALSE")
    }
    TRUE
})

#' @export
#' @importFrom BiocGenerics dbconn
setMethod("dbconn", "ParquetArraySeed", function(x) callGeneric(x@table))

#' @export
setMethod("dim", "ParquetArraySeed", function(x) {
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
setMethod("dimnames", "ParquetArraySeed", function(x) {
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
setMethod("type", "ParquetArraySeed", function(x) {
    unname(coltypes(x@table))
})

#' @export
#' @importFrom BiocGenerics type<-
setReplaceMethod("type", "ParquetArraySeed", function(x, value) {
    table <- x@table
    coltypes(table) <- value
    initialize2(x, table = table, check = FALSE)
})

#' @export
#' @importFrom SparseArray is_nonzero
setMethod("is_nonzero", "ParquetArraySeed", function(x) {
    initialize2(x, table = callGeneric(x@table), check = FALSE)
})

#' @export
#' @importFrom SparseArray nzcount
setMethod("nzcount", "ParquetArraySeed", function(x) {
    callGeneric(x@table)
})
#' @export
#' @importFrom S4Arrays is_sparse
setMethod("is_sparse", "ParquetArraySeed", function(x) {
    callGeneric(x@table)
})

#' @export
#' @importFrom BiocGenerics aperm
setMethod("aperm", "ParquetArraySeed", function(a, perm, ...) {
    k <- nkey(a@table)
    if ((length(perm) != k) || !setequal(perm, seq_len(k))) {
        stop("'perm' must be a permutation of 1:", k)
    }
    a@table@key <- a@table@key[perm]
    a
})

#' @export
#' @importFrom BiocGenerics t
setMethod("t", "ParquetArraySeed", function(x) {
    if (nkey(x@table) != 2L) {
        stop("'t()' is only defined for 2-dimensional Parquet arrays")
    }
    aperm(x, perm = 2:1)
})

.subset_ParquetArraySeed <- function(x, Nindex, drop) {
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
setMethod("[", "ParquetArraySeed", function(x, i, j, ..., drop = TRUE) {
    Nindex <- S4Arrays:::extract_Nindex_from_syscall(sys.call(), parent.frame())
    .subset_ParquetArraySeed(x, Nindex = Nindex, drop = drop)
})

#' @export
setMethod("Ops", c(e1 = "ParquetArraySeed", e2 = "ParquetArraySeed"), function(e1, e2) {
    if (!isTRUE(all.equal(e1@table, e2@table)) || !identical(e1@drop, e2@drop)) {
        stop("can only perform arithmetic operations with compatible objects")
    }
    initialize2(e1, table = callGeneric(e1@table, e2@table), check = FALSE)
})

#' @export
setMethod("Ops", c(e1 = "ParquetArraySeed", e2 = "atomic"), function(e1, e2) {
    initialize2(e1, table = callGeneric(e1@table, e2), check = FALSE)
})

#' @export
setMethod("Ops", c(e1 = "atomic", e2 = "ParquetArraySeed"), function(e1, e2) {
    initialize2(e2, table = callGeneric(e1, e2@table), check = FALSE)
})

#' @export
setMethod("Math", "ParquetArraySeed", function(x) {
    initialize2(x, table = callGeneric(x@table), check = FALSE)
})

#' @export
setMethod("Summary", "ParquetArraySeed", function(x, ..., na.rm = FALSE) {
    callGeneric(x@table)
})

#' @export
#' @importFrom BiocGenerics mean
setMethod("mean", "ParquetArraySeed", function(x, ...) {
    callGeneric(x@table)
})

#' @exportS3Method stats::median
#' @importFrom stats median
median.ParquetArraySeed <- function(x, na.rm = FALSE, ...) {
    median(x@table, na.rm = na.rm, ...)
}

#' @exportS3Method stats::quantile
#' @importFrom stats quantile
quantile.ParquetArraySeed <-
function(x, probs = seq(0, 1, 0.25), na.rm = FALSE, names = TRUE, type = 7, digits = 7, ...) {
    quantile(x@table, probs = probs, na.rm = na.rm, names = names, type = type, digits = digits, ...)
}

#' @export
#' @importFrom BiocGenerics var
setMethod("var", "ParquetArraySeed", function(x, y = NULL, na.rm = FALSE, use)  {
    callGeneric(x@table)
})

#' @export
#' @importFrom BiocGenerics sd
setMethod("sd", "ParquetArraySeed", function(x, na.rm = FALSE) {
    callGeneric(x@table)
})

#' @export
#' @importFrom BiocGenerics mad
setMethod("mad", "ParquetArraySeed",
function(x, center = median(x), constant = 1.4826, na.rm = FALSE, low = FALSE, high = FALSE) {
    callGeneric(x@table)
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
                index[[i]] <- table@key[[i]]
            } else {
                index[[i]] <- table@key[[i]][idx]
            }
        }
        # Add dropped dimensions if data are present
        if ((length(index) < nkey(table)) && all(lengths(index, use.names = FALSE) > 0L)) {
            key <- keydimnames(table)
            key[names(index)] <- index
            index <- key
        }
    }

    index
}

#' @export
#' @importFrom S4Arrays extract_array
setMethod("extract_array", "ParquetArraySeed", function(x, index) {
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
setMethod("extract_sparse_array", "ParquetArraySeed", function(x, index) {
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
setMethod("DelayedArray", "ParquetArraySeed", function(seed) ParquetArray(seed))

#' @export
#' @importFrom dplyr select
#' @importFrom S4Vectors new2
#' @importFrom stats setNames
#' @rdname ParquetArraySeed
ParquetArraySeed <- function(conn, key, fact, type = NULL, ...) {
    if (is.null(type)) {
        table <- ParquetFactTable(conn, key = key, fact = fact, ...)
        column <- as.data.frame(select(head(table@conn, 0L), !!fact))[[fact]]
        type <- .get_type(column)
    } else {
        table <- ParquetFactTable(conn, key = key, fact = fact, type = setNames(type, fact), ...)
    }
    new2("ParquetArraySeed", table = table, drop = FALSE, check = FALSE)
}
