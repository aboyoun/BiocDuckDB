#' ParquetColumn objects
#'
#' @author Patrick Aboyoun
#'
#' @include ParquetFactTable.R
#'
#' @aliases
#' ParquetColumn-class
#' as.vector,ParquetColumn-method
#' dbconn,ParquetColumn-method
#' extractROWS,ParquetColumn,ANY-method
#' head,ParquetColumn-method
#' is_nonzero,ParquetColumn-method
#' length,ParquetColumn-method
#' names,ParquetColumn-method
#' nzcount,ParquetColumn-method
#' show,ParquetColumn-method
#' showAsCell,ParquetColumn-method
#' tail,ParquetColumn-method
#' type,ParquetColumn-method
#' type<-,ParquetColumn-method
#' Ops,ParquetColumn,ParquetColumn-method
#' Ops,ParquetColumn,atomic-method
#' Ops,atomic,ParquetColumn-method
#' Math,ParquetColumn-method
#' Summary,ParquetColumn-method
#' mean,ParquetColumn-method
#' median.ParquetColumn
#' quantile.ParquetColumn
#' var,ParquetColumn,ANY-method
#' sd,ParquetColumn-method
#' mad,ParquetColumn-method
#'
#' @name ParquetColumn
NULL

#' @export
#' @importClassesFrom S4Vectors Vector
setClass("ParquetColumn", contains = "Vector", slots = c(table = "ParquetFactTable"))

#' @importFrom S4Vectors isTRUEorFALSE setValidity2
setValidity2("ParquetColumn", function(x) {
    table <- x@table
    if (ncol(table) != 1L) {
        return("'table' slot must be a single-column ParquetFactTable")
    }
    if (nkey(table) != 1L) {
        return("'table' slot must have a 'key' with a named list containing a single named character vector")
    }
    TRUE
})

#' @export
#' @importFrom S4Vectors classNameForDisplay
setMethod("show", "ParquetColumn", function(object) {
    len <- length(object)
    cat(sprintf("%s of length %s\n", classNameForDisplay(object), len))
    if (.has.row_number(object@table)) {
        n1 <- 4L
        n2 <- 0L
    } else {
        n1 <- n2 <- 2L
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
setMethod("showAsCell", "ParquetColumn", function(object) {
    callGeneric(as.vector(object@table))
})

#' @export
#' @importFrom BiocGenerics dbconn
setMethod("dbconn", "ParquetColumn", function(x) callGeneric(x@table))

#' @export
setMethod("length", "ParquetColumn", function(x) nrow(x@table))

#' @export
setMethod("names", "ParquetColumn", function(x) keydimnames(x@table)[[1L]])

#' @export
#' @importFrom BiocGenerics type
setMethod("type", "ParquetColumn", function(x) {
    unname(coltypes(x@table))
})

#' @export
#' @importFrom BiocGenerics type<-
setReplaceMethod("type", "ParquetColumn", function(x, value) {
    table <- x@table
    coltypes(table) <- value
    initialize2(x, table = table, check = FALSE)
})

#' @export
#' @importFrom SparseArray is_nonzero
setMethod("is_nonzero", "ParquetColumn", function(x) {
    initialize2(x, table = callGeneric(x@table), check = FALSE)
})

#' @export
#' @importFrom SparseArray nzcount
setMethod("nzcount", "ParquetColumn", function(x) {
    callGeneric(x@table)
})

#' @export
setMethod("extractROWS", "ParquetColumn", function(x, i) {
    if (is(i, "ParquetColumn")) {
        i <- i@table
    }
    i <- setNames(list(i), keynames(x@table))
    initialize2(x, table = .subset_ParquetFactTable(x@table, i = i), check = FALSE)
})

#' @export
#' @importFrom S4Vectors head isSingleNumber
setMethod("head", "ParquetColumn", function(x, n = 6L, ...) {
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
setMethod("tail", "ParquetColumn", function(x, n = 6L, ...) {
    if (!isSingleNumber(n)) {
        stop("'n' must be a single number")
    }
    if ((n > 0L) && .has.row_number(x)) {
        stop("tail requires a key to be efficient")
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
setMethod("Ops", c(e1 = "ParquetColumn", e2 = "ParquetColumn"), function(e1, e2) {
    initialize2(e1, table = callGeneric(e1@table, e2@table), check = FALSE)
})

#' @export
setMethod("Ops", c(e1 = "ParquetColumn", e2 = "atomic"), function(e1, e2) {
    initialize2(e1, table = callGeneric(e1@table, e2), check = FALSE)
})

#' @export
setMethod("Ops", c(e1 = "atomic", e2 = "ParquetColumn"), function(e1, e2) {
    initialize2(e2, table = callGeneric(e1, e2@table), check = FALSE)
})

#' @export
setMethod("Math", "ParquetColumn", function(x) {
    initialize2(x, table = callGeneric(x@table), check = FALSE)
})

#' @export
setMethod("Summary", "ParquetColumn", function(x, ..., na.rm = FALSE) {
    callGeneric(x@table)
})

#' @export
#' @importFrom BiocGenerics mean
setMethod("mean", "ParquetColumn", function(x, ...) {
    callGeneric(x@table)
})

#' @exportS3Method stats::median
#' @importFrom stats median
median.ParquetColumn <- function(x, na.rm = FALSE, ...) {
    median(x@table, na.rm = na.rm, ...)
}

#' @exportS3Method stats::quantile
#' @importFrom stats quantile
quantile.ParquetColumn <-
function(x, probs = seq(0, 1, 0.25), na.rm = FALSE, names = TRUE, type = 7, digits = 7, ...) {
    quantile(x@table, probs = probs, na.rm = na.rm, names = names, type = type, digits = digits, ...)
}

#' @export
#' @importFrom BiocGenerics var
setMethod("var", "ParquetColumn", function(x, y = NULL, na.rm = FALSE, use)  {
    callGeneric(x@table)
})

#' @export
#' @importFrom BiocGenerics sd
setMethod("sd", "ParquetColumn", function(x, na.rm = FALSE) {
    callGeneric(x@table)
})

#' @export
#' @importFrom BiocGenerics mad
setMethod("mad", "ParquetColumn",
function(x, center = median(x), constant = 1.4826, na.rm = FALSE, low = FALSE, high = FALSE) {
    callGeneric(x@table)
})

#' @export
#' @importFrom BiocGenerics as.vector
#' @importFrom stats setNames
setMethod("as.vector", "ParquetColumn", function(x, mode = "any") {
    df <- as.data.frame(x@table)
    vec <- setNames(df[[colnames(x@table)]], df[[keynames(x@table)[[1L]]]])
    vec <- vec[rownames(x@table)]
    if (mode != "any") {
        storage.mode(vec) <- mode
    }
    vec
})
