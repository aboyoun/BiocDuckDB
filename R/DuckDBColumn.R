#' DuckDBColumn objects
#'
#' @author Patrick Aboyoun
#'
#' @include DuckDBTable.R
#'
#' @aliases
#' DuckDBColumn-class
#' as.vector,DuckDBColumn-method
#' dbconn,DuckDBColumn-method
#' extractROWS,DuckDBColumn,ANY-method
#' head,DuckDBColumn-method
#' is_nonzero,DuckDBColumn-method
#' length,DuckDBColumn-method
#' names,DuckDBColumn-method
#' nzcount,DuckDBColumn-method
#' show,DuckDBColumn-method
#' showAsCell,DuckDBColumn-method
#' tail,DuckDBColumn-method
#' type,DuckDBColumn-method
#' type<-,DuckDBColumn-method
#' Ops,DuckDBColumn,DuckDBColumn-method
#' Ops,DuckDBColumn,atomic-method
#' Ops,atomic,DuckDBColumn-method
#' Math,DuckDBColumn-method
#' Summary,DuckDBColumn-method
#' mean,DuckDBColumn-method
#' median.DuckDBColumn
#' quantile.DuckDBColumn
#' var,DuckDBColumn,ANY-method
#' sd,DuckDBColumn-method
#' mad,DuckDBColumn-method
#' IQR,DuckDBColumn-method
#'
#' @name DuckDBColumn
NULL

#' @export
#' @importClassesFrom S4Vectors Vector
setClass("DuckDBColumn", contains = "Vector", slots = c(table = "DuckDBTable"))

#' @importFrom S4Vectors isTRUEorFALSE setValidity2
setValidity2("DuckDBColumn", function(x) {
    table <- x@table
    if (ncol(table) != 1L) {
        return("'table' slot must be a single-column DuckDBTable")
    }
    if (nkey(table) != 1L) {
        return("'table' slot must have a 'keycols' with a named list containing a single named character vector")
    }
    TRUE
})

#' @export
#' @importFrom S4Vectors classNameForDisplay
setMethod("show", "DuckDBColumn", function(object) {
    len <- length(object)
    cat(sprintf("%s of length %s\n", classNameForDisplay(object), len))
    if (.has.row_number(object@table)) {
        n1 <- 5L
        n2 <- 0L
    } else {
        n1 <- 3L
        n2 <- 2L
    }
    if (len <= n1 + n2 + 1L) {
        vec <- as.vector(object)
    } else {
        if (n2 == 0L) {
            vec <- as.vector(head(object, n1))
            if (is.character(vec)) {
                vec <- setNames(sprintf("\"%s\"", vec), names(vec))
            }
            vec <- format(vec, justify = "right")
            vec <- c(vec, "..." = "...")
        } else {
            i <- c(seq_len(n1), (len + 1L) - rev(seq_len(n2)))
            vec <- as.vector(object[i])
            if (is.character(vec)) {
                vec <- setNames(sprintf("\"%s\"", vec), names(vec))
            }
            vec <- format(vec, justify = "right")
            vec <- c(head(vec, n1), "..." = "...", tail(vec, n2))
        }
    }
    print(vec, quote = FALSE)
    invisible(NULL)
})

#' @export
#' @importFrom S4Vectors showAsCell
setMethod("showAsCell", "DuckDBColumn", function(object) {
    callGeneric(as.vector(object@table))
})

#' @export
#' @importFrom BiocGenerics dbconn
setMethod("dbconn", "DuckDBColumn", function(x) callGeneric(x@table))

#' @export
setMethod("length", "DuckDBColumn", function(x) nrow(x@table))

#' @export
setMethod("names", "DuckDBColumn", function(x) keydimnames(x@table)[[1L]])

#' @export
#' @importFrom BiocGenerics type
setMethod("type", "DuckDBColumn", function(x) {
    unname(coltypes(x@table))
})

#' @export
#' @importFrom BiocGenerics type<-
setReplaceMethod("type", "DuckDBColumn", function(x, value) {
    table <- x@table
    coltypes(table) <- value
    initialize2(x, table = table, check = FALSE)
})

#' @export
#' @importFrom SparseArray is_nonzero
setMethod("is_nonzero", "DuckDBColumn", function(x) {
    initialize2(x, table = callGeneric(x@table), check = FALSE)
})

#' @export
#' @importFrom SparseArray nzcount
setMethod("nzcount", "DuckDBColumn", function(x) {
    callGeneric(x@table)
})

#' @export
setMethod("extractROWS", "DuckDBColumn", function(x, i) {
    if (is(i, "DuckDBColumn")) {
        i <- i@table
    }
    i <- setNames(list(i), names(x@table@keycols))
    initialize2(x, table = .subset_DuckDBTable(x@table, i = i), check = FALSE)
})

#' @export
#' @importFrom S4Vectors head isSingleNumber
setMethod("head", "DuckDBColumn", function(x, n = 6L, ...) {
    if (!isSingleNumber(n)) {
        stop("'n' must be a single number")
    }
    if (.has.row_number(x@table)) {
        return(initialize2(x, table = .head_conn(x@table, n), check = FALSE))
    }
    n <- as.integer(n)
    len <- length(x)
    if (n < 0) {
        n <- max(0L, len + n)
    }
    if (n > len) {
        x
    } else {
        extractROWS(x, seq_len(n))
    }
})

#' @export
#' @importFrom S4Vectors isSingleNumber tail
setMethod("tail", "DuckDBColumn", function(x, n = 6L, ...) {
    if (!isSingleNumber(n)) {
        stop("'n' must be a single number")
    }
    if ((n > 0L) && .has.row_number(x)) {
        stop("tail requires a keycols to be efficient")
    }
    n <- as.integer(n)
    len <- length(x)
    if (n < 0) {
        n <- max(0L, len + n)
    }
    if (n > len) {
        x
    } else {
        extractROWS(x, (len + 1L) - rev(seq_len(n)))
    }
})

#' @export
setMethod("Ops", c(e1 = "DuckDBColumn", e2 = "DuckDBColumn"), function(e1, e2) {
    initialize2(e1, table = callGeneric(e1@table, e2@table), check = FALSE)
})

#' @export
setMethod("Ops", c(e1 = "DuckDBColumn", e2 = "atomic"), function(e1, e2) {
    initialize2(e1, table = callGeneric(e1@table, e2), check = FALSE)
})

#' @export
setMethod("Ops", c(e1 = "atomic", e2 = "DuckDBColumn"), function(e1, e2) {
    initialize2(e2, table = callGeneric(e1, e2@table), check = FALSE)
})

#' @export
setMethod("Math", "DuckDBColumn", function(x) {
    initialize2(x, table = callGeneric(x@table), check = FALSE)
})

#' @export
setMethod("Summary", "DuckDBColumn", function(x, ..., na.rm = FALSE) {
    callGeneric(x@table)
})

#' @export
#' @importFrom BiocGenerics mean
setMethod("mean", "DuckDBColumn", function(x, ...) {
    callGeneric(x@table)
})

#' @exportS3Method stats::median
#' @importFrom stats median
median.DuckDBColumn <- function(x, na.rm = FALSE, ...) {
    median(x@table, na.rm = na.rm, ...)
}

#' @exportS3Method stats::quantile
#' @importFrom stats quantile
quantile.DuckDBColumn <-
function(x, probs = seq(0, 1, 0.25), na.rm = FALSE, names = TRUE, type = 7, digits = 7, ...) {
    quantile(x@table, probs = probs, na.rm = na.rm, names = names, type = type, digits = digits, ...)
}

#' @export
#' @importFrom BiocGenerics var
setMethod("var", "DuckDBColumn", function(x, y = NULL, na.rm = FALSE, use)  {
    callGeneric(x@table)
})

#' @export
#' @importFrom BiocGenerics sd
setMethod("sd", "DuckDBColumn", function(x, na.rm = FALSE) {
    callGeneric(x@table)
})

#' @export
#' @importFrom BiocGenerics mad
setMethod("mad", "DuckDBColumn",
function(x, center = median(x), constant = 1.4826, na.rm = FALSE, low = FALSE, high = FALSE) {
    callGeneric(x@table, constant = constant)
})

#' @export
#' @importFrom BiocGenerics IQR
setMethod("IQR", "DuckDBColumn", function(x, na.rm = FALSE, type = 7) {
    callGeneric(x@table, type = type)
})

#' @export
#' @importFrom BiocGenerics as.vector
#' @importFrom stats setNames
setMethod("as.vector", "DuckDBColumn", function(x, mode = "any") {
    df <- as.data.frame(x@table)
    vec <- setNames(df[[colnames(x@table)]], df[[names(x@table@keycols)]])
    vec <- vec[rownames(x@table)]
    if (mode != "any") {
        storage.mode(vec) <- mode
    }
    vec
})
