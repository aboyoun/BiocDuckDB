#' Parquet-backed DataFrame
#'
#' Create a Parquet-backed \linkS4class{DataFrame}, where the data are kept on disk until requested.
#'
#' @inheritParams ParquetFactTable
#'
#' @return A ParquetDataFrame where each column is a \linkS4class{ParquetColumn}.
#'
#' @details
#' The ParquetDataFrame is essentially just a \linkS4class{DataFrame} of \linkS4class{ParquetColumn} objects.
#' It is primarily useful for indicating that the in-memory representation is consistent with the underlying Parquet data
#' (e.g., no delayed filter/mutate operations have been applied, no data has been added from other files).
#' Thus, users can specialize code paths for a ParquetDataFrame to operate directly on the underlying Parquet data.
#'
#' @author Aaron Lun, Patrick Aboyoun
#'
#' @examples
#' # Mocking up a file:
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' arrow::write_parquet(cbind(model = rownames(mtcars), mtcars), tf)
#'
#' # Creating our Parquet-backed data frame:
#' df <- ParquetDataFrame(tf, key = "model")
#' df
#'
#' # Extraction yields a ParquetColumn:
#' df$carb
#'
#' # Slicing ParquetDataFrame objects:
#' df[,1:5]
#' df[1:5,]
#'
#' # Combining by ParquetDataFrame and ParquetColumn objects:
#' combined <- cbind(df, df)
#' class(combined)
#' combined2 <- cbind(df, some_new_name=df[,1])
#' class(combined2)
#'
#' @aliases
#' ParquetDataFrame-class
#' makeNakedCharacterMatrixForDisplay,ParquetDataFrame-method
#' show,ParquetDataFrame-method
#'
#' length,ParquetDataFrame-method
#'
#' names,ParquetDataFrame-method
#' rownames<-,ParquetDataFrame-method
#' names<-,ParquetDataFrame-method
#'
#' extractROWS,ParquetDataFrame,ANY-method
#' head,ParquetDataFrame-method
#' tail,ParquetDataFrame-method
#' extractCOLS,ParquetDataFrame-method
#' [,ParquetDataFrame,ANY,ANY,ANY-method
#' [[,ParquetDataFrame-method
#'
#' replaceROWS,ParquetDataFrame-method
#' replaceCOLS,ParquetDataFrame-method
#' normalizeSingleBracketReplacementValue,ParquetDataFrame-method
#' [[<-,ParquetDataFrame-method
#'
#' bindROWS,ParquetDataFrame-method
#' cbind,ParquetDataFrame-method
#' cbind.ParquetDataFrame
#'
#' as.data.frame,ParquetDataFrame-method
#'
#' @include acquireDataset.R
#' @include ParquetColumn.R
#' @include ParquetFactTable.R
#'
#' @name ParquetDataFrame
NULL

#' @export
#' @importClassesFrom S4Vectors DataFrame
setClass("ParquetDataFrame", contains = c("ParquetFactTable", "DataFrame"))

#' @importFrom S4Vectors setValidity2
setValidity2("ParquetDataFrame", function(x) {
    if (nkey(x) != 1L) {
        return("'key' slot must be a named list containing a single named character vector")
    }
    TRUE
})

#' @export
#' @importFrom S4Vectors makeNakedCharacterMatrixForDisplay
setMethod("makeNakedCharacterMatrixForDisplay", "ParquetDataFrame", function(x) {
    callNextMethod(as.data.frame(x))
})

#' @export
#' @importFrom S4Vectors classNameForDisplay get_showHeadLines get_showTailLines makeNakedCharacterMatrixForDisplay
setMethod("show", "ParquetDataFrame", function(object) {
    nhead <- get_showHeadLines()
    ntail <- get_showTailLines()
    x_nrow <- nrow(object)
    x_ncol <- ncol(object)
    cat(classNameForDisplay(object), " with ",
        x_nrow, " row", ifelse(x_nrow == 1L, "", "s"), " and ",
        x_ncol, " column", ifelse(x_ncol == 1L, "", "s"), "\n", sep = "")
    if (x_nrow != 0L && x_ncol != 0L) {
        if (x_nrow <= nhead + ntail + 1L) {
            m <- makeNakedCharacterMatrixForDisplay(object)
            x_rownames <- rownames(object)
            if (!is.null(x_rownames)) {
                rownames(m) <- x_rownames
            }
        } else {
            i <- c(seq_len(nhead), (x_nrow + 1L) - rev(seq_len(ntail)))
            df <- as.data.frame(object[i, , drop = FALSE])
            m <- rbind(makeNakedCharacterMatrixForDisplay(head(df, nhead)),
                       rbind(rep.int("...", x_ncol)),
                       makeNakedCharacterMatrixForDisplay(tail(df, ntail)))
            x_rownames <- c(rownames(head(object, nhead)), rownames(tail(object, ntail)))
            rownames(m) <- S4Vectors:::make_rownames_for_RectangularData_display(x_rownames, x_nrow, nhead, ntail)
        }
        m <- rbind(rep.int("<ParquetColumn>", ncol(object)), m)
        print(m, quote = FALSE, right = TRUE)
    }
    invisible(NULL)
})

#' @export
setMethod("length", "ParquetDataFrame", function(x) ncol(x))

#' @export
#' @importFrom BiocGenerics colnames
setMethod("names", "ParquetDataFrame", function(x) colnames(x))

#' @export
#' @importFrom BiocGenerics rownames<-
setReplaceMethod("rownames", "ParquetDataFrame", function(x, value) {
    keydimnames(x) <- list(value)
    x
})

#' @export
#' @importFrom BiocGenerics colnames<-
#' @importFrom S4Vectors mcols mcols<-
setReplaceMethod("names", "ParquetDataFrame", function(x, value) {
    colnames(x) <- value
    mc <- mcols(x)
    if (!is.null(mc)) {
        rownames(mcols(mc)) <- value
    }
    x
})

#' @export
#' @importFrom S4Vectors extractROWS
#' @importFrom stats setNames
setMethod("extractROWS", "ParquetDataFrame", function(x, i) {
    if (missing(i)) {
        return(x)
    }
    i <- setNames(list(i), keynames(x))
    .subset_ParquetFactTable(x, i = i)
})

#' @export
#' @importFrom S4Vectors head isSingleNumber
setMethod("head", "ParquetDataFrame", function(x, n = 6L, ...) {
    if (!isSingleNumber(n)) {
        stop("'n' must be a single number")
    }
    n <- as.integer(n)
    nr <- nrow(x)
    if (n < 0) {
        n <- max(0L, nr + n)
    }
    if (n > nr) {
        x
    } else {
        extractROWS(x, seq_len(n))
    }
})

#' @export
#' @importFrom S4Vectors isSingleNumber tail
setMethod("tail", "ParquetDataFrame", function(x, n = 6L, ...) {
    if (!isSingleNumber(n)) {
        stop("'n' must be a single number")
    }
    n <- as.integer(n)
    nr <- nrow(x)
    if (n < 0) {
        n <- max(0L, nr + n)
    }
    if (n > nr) {
        x
    } else {
        extractROWS(x, (nr + 1L) - rev(seq_len(n)))
    }
})

#' @export
#' @importFrom stats setNames
#' @importFrom S4Vectors extractCOLS mcols normalizeSingleBracketSubscript
setMethod("extractCOLS", "ParquetDataFrame", function(x, i) {
    if (missing(i)) {
        return(x)
    }
    xstub <- setNames(seq_along(x), names(x))
    i <- normalizeSingleBracketSubscript(i, xstub)
    if (anyDuplicated(i)) {
        stop("cannot extract duplicate columns in a ParquetDataFrame")
    }
    mc <- extractROWS(mcols(x), i)
    .subset_ParquetFactTable(x, j = i, elementMetadata = mc)
})

#' @export
setMethod("[", "ParquetDataFrame", function(x, i, j, ..., drop = TRUE) {
    if (!missing(j)) {
        x <- extractCOLS(x, j)
    }
    if (!missing(i)) {
        x <- extractROWS(x, i)
    }
    if (missing(drop)) {
        drop <- (ncol(x) == 1L)
    }
    if (drop && (ncol(x) == 1L)) {
        x <- x[[1L]]
    }
    x
})

#' @export
#' @importFrom S4Vectors new2 normalizeDoubleBracketSubscript
setMethod("[[", "ParquetDataFrame", function(x, i, j, ...) {
    if (!missing(j)) {
        stop("list-style indexing of a ParquetDataFrame with non-missing 'j' is not supported")
    }

    if (missing(i) || length(i) != 1L) {
        stop("expected a length-1 'i' for list-style indexing of a ParquetDataFrame")
    }

    i <- normalizeDoubleBracketSubscript(i, x)
    column <- extractCOLS(x, i)
    new2("ParquetColumn", table = as(column, "ParquetFactTable"), metadata = as.list(mcols(column)), check = FALSE)
})

#' @export
#' @importFrom S4Vectors replaceROWS
setMethod("replaceROWS", "ParquetDataFrame", function(x, i, value) {
    stop("replacement of rows in a ParquetDataFrame is not supported")
})

#' @export
#' @importFrom S4Vectors new2 normalizeSingleBracketReplacementValue
setMethod("normalizeSingleBracketReplacementValue", "ParquetDataFrame", function(value, x) {
    if (is(value, "ParquetColumn")) {
        return(new2("ParquetDataFrame", value@table, check = FALSE))
    }
    callNextMethod()
})

#' @export
#' @importFrom stats setNames
#' @importFrom S4Vectors replaceCOLS normalizeSingleBracketSubscript
setMethod("replaceCOLS", "ParquetDataFrame", function(x, i, value) {
    xstub <- setNames(seq_along(x), names(x))
    i2 <- normalizeSingleBracketSubscript(i, xstub, allow.NAs = TRUE)
    if (!anyNA(i2)) {
        if (is(value, "ParquetDataFrame")) {
            if (isTRUE(all.equal(x, value))) {
                x@fact[i2] <- unname(value@fact)
                return(x)
            }
        }
    }
    stop("not compatible ParquetDataFrame objects")
})

#' @export
#' @importFrom S4Vectors normalizeDoubleBracketSubscript
setMethod("[[<-", "ParquetDataFrame", function(x, i, j, ..., value) {
    i2 <- normalizeDoubleBracketSubscript(i, x, allow.nomatch = TRUE)
    if (length(i2) == 1L && !is.na(i2)) {
        if (is(value, "ParquetColumn")) {
            if (isTRUE(all.equal(as(x, "ParquetFactTable"), value@table))) {
                x@fact[[i2]] <- value@table@fact[[1L]]
                return(x)
            }
        }
    }
    stop("not compatible ParquetDataFrame and ParquetColumn objects")
})

#' @export
#' @importFrom S4Vectors bindROWS
setMethod("bindROWS", "ParquetDataFrame", function(x, objects = list(), use.names = TRUE, ignore.mcols = FALSE, check = TRUE) {
    stop("binding rows to a ParquetDataFrame is not supported")
})

#' @export
#' @importFrom dplyr rename
#' @importFrom S4Vectors combineRows make_zero_col_DFrame mcols mcols<- metadata new2
cbind.ParquetDataFrame <- function(..., deparse.level = 1) {
    objects <- list(...)

    all_mcols <- vector("list", length(objects))
    all_metadata <- vector("list", length(objects))
    has_mcols <- FALSE

    for (i in seq_along(objects)) {
        obj <- objects[[i]]

        md <- list()
        mc <- make_zero_col_DFrame(NCOL(obj))
        if (is(obj, "ParquetColumn")) {
            table <- obj@table
            cname <- names(objects)[i]
            if (!is.null(cname)) {
                colnames(table) <- cname
            }
            objects[[i]] <- new2("ParquetDataFrame", table, check = FALSE)
            md <- metadata(obj)
            if (length(md) > 0L) {
                has_mcols <- TRUE
                mc <- new2("DFrame", rownames = cname, nrows = 1L, listData = md, check = FALSE)
                md <- list()
            }
        } else if (is(obj, "ParquetDataFrame")) {
            mc <- mcols(obj, use.names = FALSE) %||% mc
            if (ncol(mc) > 0L) {
                has_mcols <- TRUE
                mcols(objects[[i]]) <- NULL
            }
            md <- metadata(obj)
        }
        all_mcols[[i]] <- mc
        all_metadata[[i]] <- md
    }

    x <- objects[[1L]]
    objects <- objects[-1L]
    bound <- bindCOLS(x, objects)

    if (has_mcols) {
        all_mcols <- do.call(combineRows, all_mcols)
        rownames(all_mcols) <- colnames(bound)
    } else {
        all_mcols <- NULL
    }
    all_metadata <- do.call(c, all_metadata)

    initialize2(bound, elementMetadata = all_mcols, metadata = all_metadata, check = FALSE)
}

#' @export
setMethod("cbind", "ParquetDataFrame", cbind.ParquetDataFrame)

#' @export
#' @importFrom BiocGenerics as.data.frame
#' @importFrom stats setNames
setMethod("as.data.frame", "ParquetDataFrame", function(x, row.names = NULL, optional = FALSE, ...) {
    # as.data.frame,ParquetFactTable-method
    df <- callNextMethod(x, row.names = row.names, optional = optional, ...)

    # Add rownames, renaming if specified
    if (is.null(names(x@key[[1L]]))) {
        rnames <- df[[keynames(x)]]
    } else {
        rnames <- x@key[[1L]]
        rnames <- setNames(names(rnames), rnames)
        rnames <- rnames[as.character(df[[keynames(x)]])]
    }
    rownames(df) <- rnames

    df[rownames(x), colnames(x), drop = FALSE]
})

#' @export
#' @importFrom dplyr everything select
#' @importFrom S4Vectors isSingleString new2
#' @importFrom stats setNames
#' @rdname ParquetDataFrame
ParquetDataFrame <- function(conn, key, fact, ...) {
    if (missing(fact)) {
        tbl <- ParquetFactTable(conn, key = key, ...)
    } else {
        tbl <- ParquetFactTable(conn, key = key, fact = fact, ...)
    }
    new2("ParquetDataFrame", tbl, ..., check = FALSE)
}
