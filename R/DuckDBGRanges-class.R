#' DuckDBGRanges objects
#'
#' @description
#' The DuckDBGRanges class extends the \linkS4class{GenomicRanges} virtual
#' class for DuckDB tables.
#'
#' @details
#' DuckDBGRanges adds \linkS4class{GenomicRanges} semantics to a DuckDB table.
#' This includes the ability to define the sequence names, start, end, width,
#' and strand columns, as well as the metadata columns. The \code{seqinfo}
#' slot is used to define the sequence information for the ranges.
#'
#' @section Constructor:
#' \describe{
#'   \item{\code{DuckDBGRanges(conn, seqnames, start = NULL, end = NULL, width = NULL,
#'     strand = NULL, keycol = NULL, dimtbl = NULL, mcols = NULL, seqinfo = NULL,
#'     seqlengths = NULL)}:}{
#'     Creates a DuckDBGRanges object.
#'     \describe{
#'       \item{\code{conn}}{
#'         Either a character vector containing the paths to parquet, csv, or
#'         gzipped csv data files; a string that defines a duckdb \code{read_*}
#'         data source; a DuckDBDataFrame object; or a tbl_duckdb_connection
#'         object.
#'       }
#'       \item{\code{seqnames}}{
#'         Either \code{NULL} or a string specifying the column from
#'         \code{conn} that defines the sequence names.
#'       }
#'       \item{\code{start}}{
#'         Either \code{NULL} or a string specifying the column from
#'         \code{conn} that defines the start of the range.
#'       }
#'       \item{\code{end}}{
#'         Either \code{NULL} or a string specifying the column from
#'         \code{conn} that defines the end of the range.
#'       }
#'       \item{\code{width}}{
#'         Either \code{NULL} or a string specifying the column from
#'         \code{conn} that defines the width of the range.
#'       }
#'       \item{\code{strand}}{
#'         Either \code{NULL} or a string specifying the column from
#'         \code{conn} that defines the width of the range.
#'       }
#'       \item{\code{keycol}}{
#'         An optional string specifying the column name from \code{conn} that
#'         will define the foreign key in the underlying table, or a named list
#'         containing a character vector where the name of the list element
#'         defines the foreign key and the character vector set the distinct
#'         values for that key. If missing, a \code{row_number} column is
#'         created as an identifier.
#'       }
#'       \item{\code{dimtbl}}{
#'         A optional named \code{DataFrameList} that specifies the dimension
#'         table associated with the \code{keycol}. The name of the list
#'         element must match the name of the \code{keycol} list. Additionally,
#'         the \code{DataFrame} object must have row names that match the
#'         distinct values of the \code{keycol} list element and columns
#'         that define partitions in the data table for efficient querying.
#'       }
#'       \item{\code{mcols}}{
#'         Optional character vector specifying the columns that define the
#'         metadata columns.
#'       }
#'       \item{\code{seqinfo}}{
#'         Either \code{NULL}, or a \code{\link{Seqinfo}} object, or a character
#'         vector of unique sequence names (a.k.a. seqlevels), or a named
#'         numeric vector of sequence lengths.
#'       }
#'       \item{\code{seqlengths}}{
#'         Either \code{NULL}, or an integer vector named with
#'         \code{levels(seqnames)} and containing the \code{lengths}
#'         (or \code{NA}) for each level in \code{levels(seqnames)}.
#'       }
#'     }
#'   }
#' }
#'
#' @section Accessors:
#' In the code snippets below, \code{x} is a DuckDBGRanges object:
#' \describe{
#'   \item{\code{length(x)}:}{
#'     Get the number of elements.
#'   }
#'   \item{\code{names(x)}:}{
#'     Get the names of the elements.
#'   }
#'   \item{\code{seqnames(x)}:}{
#'     Get the sequence names.
#'   }
#'   \item{\code{ranges(x)}:}{
#'     Get the ranges as a \linkS4class{DuckDBDataFrame}.
#'   }
#'   \item{\code{start(x)}:}{
#'     Get the start values as a \linkS4class{DuckDBColumn}.
#'   }
#'   \item{\code{end(x)}:}{
#'     Get the end values as a \linkS4class{DuckDBColumn}.
#'   }
#'   \item{\code{width(x)}:}{
#'     Get the width values as a \linkS4class{DuckDBColumn}.
#'   }
#'   \item{\code{strand(x)}:}{
#'     Get the strand values as a \linkS4class{DuckDBColumn}.
#'   }
#'   \item{\code{mcols(x)}, \code{mcols(x) <- value}:}{
#'     Get or set the metadata columns.
#'   }
#'   \item{\code{seqinfo(x)}:}{
#'     Get the information about the underlying sequences.
#'   }
#'   \item{\code{seqlevels(x)}:}{
#'     Get the sequence levels; equivalent to \code{seqlevels(seqinfo(x))}.
#'   }
#'   \item{\code{seqlengths(x)}:}{
#'     Get the sequence lengths; equivalent to \code{seqlengths(seqinfo(x))}.
#'   }
#'   \item{\code{isCircular(x)}:}{
#'     Get or set the circularity flags.
#'   }
#'   \item{\code{genome(x)}:}{
#'     Get the genome identifier or assembly name for each sequence; equivalent
#'     to \code{genome(seqinfo(x))}.
#'   }
#'   \item{\code{seqlevelsStyle(x)}:}{
#'     Get the seqname style for \code{x}.
#'   }
#' }
#'
#' @section Coercion:
#' \describe{
#'   \item{\code{as(from, "DuckDBDataFrame")}:}{
#'     Creates a \linkS4class{DuckDBDataFrame} object.
#'   }
#'   \item{\code{as.data.frame(x)}:}{
#'     Coerces \code{x} to a data.frame.
#'   }
#'   \item{\code{as(from, "GRanges")}:}{
#'     Converts a DuckDBGRanges object to a GRanges object. This conversion
#'     begins by transforming the DuckDBGRanges into memory using
#'     \code{as.data.frame}. A GRanges object is then instantiated using the
#'     seqnames, start, width, and strand columns from the data.frame. Lastly,
#'     the seqinfo, metadata, and mcols (metadata columns) are copied over.
#'   }
#'   \item{\code{realize(x, BACKEND = getAutoRealizationBackend())}:}{
#'     Realize an object into memory or on disk using the equivalent of
#'     \code{realize(as(x, "GRanges"), BACKEND)}.
#'   }
#' }
#'
#' @section Subsetting:
#' In the code snippets below, \code{x} is a DuckDBGRanges object:
#' \describe{
#'   \item{\code{x[i]}:}{
#'     Returns a DuckDBGRanges object containing the selected elements.
#'   }
#'   \item{\code{head(x, n = 6L)}:}{
#'     If \code{n} is non-negative, returns the first n elements of \code{x}.
#'     If \code{n} is negative, returns all but the last \code{abs(n)} elements
#'     of \code{x}.
#'   }
#'   \item{\code{tail(x, n = 6L)}:}{
#'     If \code{n} is non-negative, returns the last n elements of \code{x}.
#'     If \code{n} is negative, returns all but the first \code{abs(n)} elements
#'     of \code{x}.
#'   }
#' }
#'
#' @section Displaying:
#' The \code{show()} method for DuckDBGRanges objects obeys global options
#' \code{showHeadLines} and \code{showTailLines} for controlling the number of
#' head and tail rows to display.
#'
#' @author Patrick Aboyoun
#'
#' @examples
#' # Create an example data set with start and width columns:
#' df <- data.frame(id = head(letters, 10),
#'                  seqnames = rep.int(c("chr2", "chr2", "chr1", "chr3"), c(1, 3, 2, 4)),
#'                  start = 1:10, width = 10:1,
#'                  strand = strand(rep.int(c("-", "+", "*", "+", "-"), c(1, 2, 2, 3, 2))),
#'                  score = 1:10,
#'                  GC = seq(1, 0, length = 10))
#' tf <- tempfile(fileext = ".parquet")
#' on.exit(unlink(tf))
#' arrow::write_parquet(df, tf)
#'
#' # Create the DuckDBGRanges object
#' seqinfo <- Seqinfo(paste0("chr", 1:3), c(1000, 2000, 1500), NA, "mock1")
#' gr <- DuckDBGRanges(tf, seqnames = "seqnames", start = "start", width = "width",
#'                     strand = "strand", mcols = c("score", "GC"), seqinfo = seqinfo,
#'                     keycol = "id")
#' gr
#'
#' @aliases
#' DuckDBGRanges-class
#'
#' dbconn,DuckDBGRanges-method
#' tblconn,DuckDBGRanges-method
#' dimtbls,DuckDBGRanges-method
#' dimtbls<-,DuckDBGRanges-method
#' length,DuckDBGRanges-method
#' names,DuckDBGRanges-method
#' seqinfo,DuckDBGRanges-method
#' seqnames,DuckDBGRanges-method
#' start,DuckDBGRanges-method
#' end,DuckDBGRanges-method
#' width,DuckDBGRanges-method
#' strand,DuckDBGRanges-method
#' ranges,DuckDBGRanges-method
#' elementMetadata,DuckDBGRanges-method
#' elementMetadata<-,DuckDBGRanges-method
#'
#' DuckDBGRanges
#'
#' extractROWS,DuckDBGRanges,ANY-method
#' [,DuckDBGRanges,ANY,ANY,ANY-method
#' head,DuckDBGRanges-method
#' tail,DuckDBGRanges-method
#'
#' coerce,DuckDBGRanges,DuckDBDataFrame-method
#' as.data.frame,DuckDBGRanges-method
#' coerce,DuckDBGRanges,GRanges-method
#' realize,DuckDBGRanges-method
#'
#' show,DuckDBGRanges-method
#'
#' @include DuckDBDataFrame-class.R
#'
#' @keywords classes methods
#'
#' @name DuckDBGRanges-class
NULL

.datacols_granges <- expression(seqnames = NULL, start = NULL, end = NULL, width = NULL, strand = NULL)

#' @export
#' @importClassesFrom GenomicRanges GenomicRanges
#' @importClassesFrom GenomeInfoDb Seqinfo
#' @importFrom S4Vectors new2
setClass("DuckDBGRanges", contains = "GenomicRanges",
         slots = c(frame = "DuckDBDataFrame", seqinfo = "Seqinfo"),
         prototype = prototype(frame = new2("DuckDBDataFrame", datacols = .datacols_granges, check = FALSE)))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

#' @export
#' @importFrom BiocGenerics dbconn
setMethod("dbconn", "DuckDBGRanges", function(x) callGeneric(x@frame))

#' @export
setMethod("tblconn", "DuckDBGRanges", function(x, filter = TRUE) {
    callGeneric(x@frame, filter = filter)
})

setMethod(".keycols", "DuckDBGRanges", function(x) callGeneric(x@frame))

#' @export
setMethod("dimtbls", "DuckDBGRanges", function(x) callGeneric(x@frame))

#' @export
setReplaceMethod("dimtbls", "DuckDBGRanges", function(x, value) callGeneric(x@frame, value))

setMethod(".has_row_number", "DuckDBGRanges", function(x) callGeneric(x@frame))

#' @export
setMethod("length", "DuckDBGRanges", function(x) nrow(x@frame))

#' @export
setMethod("names", "DuckDBGRanges", function(x) rownames(x@frame))

#' @export
#' @importFrom GenomeInfoDb seqinfo
setMethod("seqinfo", "DuckDBGRanges", function(x) x@seqinfo)

#' @export
#' @importFrom GenomeInfoDb seqnames
setMethod("seqnames", "DuckDBGRanges", function(x) x@frame[["seqnames"]])

#' @export
#' @importFrom BiocGenerics start
setMethod("start", "DuckDBGRanges", function(x, ...) x@frame[["start"]])

#' @export
#' @importFrom BiocGenerics end
setMethod("end", "DuckDBGRanges", function(x, ...) x@frame[["end"]])

#' @export
#' @importFrom BiocGenerics width
setMethod("width", "DuckDBGRanges", function(x) x@frame[["width"]])

#' @export
#' @importFrom BiocGenerics strand
setMethod("strand", "DuckDBGRanges", function(x, ...) x@frame[["strand"]])

#' @export
#' @importFrom IRanges ranges
setMethod("ranges", "DuckDBGRanges", function (x, use.names = TRUE, use.mcols = FALSE, ...) {
    df <- x@frame[, c("start", "end", "width"), drop = FALSE]
    if (use.mcols && !is.null(x@elementMetadata)) {
        df <- cbind.DuckDBDataFrame(df, x@elementMetadata)
    }
    df
})

#' @export
#' @importFrom S4Vectors elementMetadata
setMethod("elementMetadata", "DuckDBGRanges", function(x) x@elementMetadata)

#' @export
#' @importFrom S4Vectors elementMetadata<-
setReplaceMethod("elementMetadata", "DuckDBGRanges", function(x, ..., value) {
    if (!is(value, "DuckDBDataFrame") || !isTRUE(all.equal(x@frame, value))) {
        stop("'elementMetadata' must be a compatible DuckDBDataFrame object")
    }
    replaceSlots(x, elementMetadata = value, check = FALSE)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Validity
###

#' @importFrom S4Vectors mcols
setValidity2("DuckDBGRanges", function(x) {
    msg <- NULL
    frame <- x@frame
    if (!setequal(colnames(frame), names(.datacols_granges))) {
        msg <- c(msg, "missing definition for one or more of 'seqnames', 'start', 'end', 'width', and 'strand'")
    }
    mcols <- x@elementMetadata
    if (!is.null(mcols) && !is(mcols, "DuckDBDataFrame") && !isTRUE(all.equal(mcols, frame))) {
        msg <- c(msg, "'mcols' must be compatible with its associated ranges")
    }
    msg %||% TRUE
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

#' @export
#' @importFrom dplyr distinct pull select
#' @importFrom GenomeInfoDb Seqinfo
#' @importFrom S4Vectors isSingleString new2
#' @importFrom stats setNames
DuckDBGRanges <-
function(conn, seqnames, start = NULL, end = NULL, width = NULL, strand = NULL,
         keycol = NULL, dimtbl = NULL, mcols = NULL, seqinfo = NULL,
         seqlengths = NULL)
{
    datacols <- .datacols_granges

    stringAsName <- function(x) if (isSingleString(x)) as.name(x) else x

    nargs_range <- sum(c(!is.null(start), !is.null(end), !is.null(width)))
    if ((nargs_range == 1L) && !is.null(start)) {
        end <- start
        width <- 1L
    } else if (nargs_range != 2L) {
        stop("must provide exactly two of 'start', 'end', and 'width'")
    }

    datacols[["seqnames"]] <- stringAsName(seqnames) %||% ""
    datacols[["start"]] <- stringAsName(start)
    datacols[["end"]] <- stringAsName(end)
    datacols[["width"]] <- stringAsName(width)
    datacols[["strand"]] <- stringAsName(strand) %||% "*"

    if (is.null(datacols[["start"]])) {
        datacols[["start"]] <- call("+", call("-", datacols[["end"]], datacols[["width"]]), 1L)
    } else if (is.null(datacols[["end"]])) {
        datacols[["end"]] <- call("-", call("+", datacols[["start"]], datacols[["width"]]), 1L)
    } else if (is.null(datacols[["width"]])) {
        datacols[["width"]] <- call("+", call("-", datacols[["end"]], datacols[["start"]]), 1L)
    }

    datacols <- datacols[c("seqnames", "start", "end", "width", "strand")]
    ccols <- datacols
    if (length(mcols) == 0L) {
        mcols <- NULL
    } else {
        if (is.character(mcols)) {
            mcols <- sapply(mcols, as.name, simplify = FALSE)
        }
        mcols <- as.expression(mcols)
        ccols <- c(ccols, mcols)
    }

    comb <- DuckDBDataFrame(conn, datacols = ccols, keycol = keycol, dimtbl = dimtbl)
    frame <- comb[, names(datacols), drop = FALSE]
    if (is.null(mcols)) {
        mcols <- comb[, character(), drop = FALSE]
    } else {
        mcols <- comb[, names(mcols), drop = FALSE]
    }

    seqinfo <- GenomicRanges:::normarg_seqinfo2(seqinfo, seqlengths)
    if (is.null(seqinfo)) {
        conn <- tblconn(frame)
        seqinfo <- Seqinfo(sort(pull(distinct(select(conn, !!!as.list(seqnames))))))
    } else {
        seqinfo <- GenomicRanges:::normarg_seqinfo1(seqinfo)
    }

    new2("DuckDBGRanges", frame = frame, seqinfo = seqinfo, elementMetadata = mcols, check = FALSE)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Subsetting
###

#' @export
#' @importFrom S4Vectors extractROWS
setMethod("extractROWS", "DuckDBGRanges", function(x, i) {
    if (missing(i)) {
        return(x)
    }

    frame <- x@frame
    frame <- callGeneric(frame, i = i)

    mcols <- x@elementMetadata
    if (!is.null(mcols)) {
        mcols <- replaceSlots(frame, datacols = mcols@datacols, check = FALSE)
    }

    replaceSlots(x, frame = frame, elementMetadata = mcols, check = FALSE)
})

#' @export
setMethod("[", "DuckDBGRanges", function(x, i, j, ..., drop = TRUE) {
    frame <- x@frame
    if (!missing(i)) {
        frame <- extractROWS(frame, i)
    }

    mcols <- x@elementMetadata
    if (!is.null(mcols)) {
        mcols <- replaceSlots(frame, datacols = mcols@datacols, check = FALSE)
    }

    replaceSlots(x, frame = frame, elementMetadata = mcols, check = FALSE)
})

#' @export
#' @importFrom S4Vectors head
setMethod("head", "DuckDBGRanges", function(x, n = 6L, ...) {
    frame <- x@frame
    frame <- callGeneric(frame, n = n, ...)

    mcols <- x@elementMetadata
    if (!is.null(mcols)) {
        mcols <- replaceSlots(frame, datacols = mcols@datacols, check = FALSE)
    }

    replaceSlots(x, frame = frame, elementMetadata = mcols, check = FALSE)
})

#' @export
#' @importFrom S4Vectors tail
setMethod("tail", "DuckDBGRanges", function(x, n = 6L, ...) {
    frame <- x@frame
    frame <- callGeneric(frame, n = n, ...)

    mcols <- x@elementMetadata
    if (!is.null(mcols)) {
        mcols <- replaceSlots(frame, datacols = mcols@datacols, check = FALSE)
    }

    replaceSlots(x, frame = frame, elementMetadata = mcols, check = FALSE)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

#' @export
setAs("DuckDBGRanges", "DuckDBDataFrame", function(from) {
    df <- from@frame
    if (ncol(from@elementMetadata) > 0L) {
        df <- cbind.DuckDBDataFrame(df, from@elementMetadata)
    }
    df
})

#' @export
#' @importFrom BiocGenerics as.data.frame
setMethod("as.data.frame", "DuckDBGRanges",
function(x, row.names = NULL, optional = FALSE, ...) {
    df <- as(x, "DuckDBDataFrame")
    callGeneric(df, row.names = row.names, optional = optional, ...)
})

#' @export
#' @importClassesFrom GenomicRanges GRanges
#' @importClassesFrom S4Vectors DFrame
#' @importFrom BiocGenerics strand
#' @importFrom GenomeInfoDb seqinfo
#' @importFrom GenomicRanges GRanges
#' @importFrom IRanges IRanges
#' @importFrom S4Vectors mcols mcols<- metadata metadata<- Rle
setAs("DuckDBGRanges", "GRanges", function(from) {
    df <- as.data.frame(from)

    seqnames <- Rle(df[["seqnames"]])
    ranges <- IRanges(start = df[["start"]], width = df[["width"]])
    if (!.has_row_number(from)) {
        names(ranges) <- rownames(df)
    }
    strand <- strand(df[["strand"]])
    gr <- GRanges(seqnames, ranges = ranges, strand = strand,
                  seqinfo = seqinfo(from))

    metadata(gr) <- metadata(from)
    mc <- mcols(from)
    if (ncol(mc) > 0L) {
        mcols(gr) <- as(df[, colnames(mc), drop = FALSE], "DFrame")
    }

    gr
})

#' @importClassesFrom GenomicRanges GRanges
#' @importFrom DelayedArray getAutoRealizationBackend realize
setMethod("realize", "DuckDBGRanges",
function(x, BACKEND = getAutoRealizationBackend()) {
    x <- as(x, "GRanges")
    if (!is.null(BACKEND)) {
        x <- callGeneric(x, BACKEND = BACKEND)
    }
    x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Display
###

#' @export
setMethod("show", "DuckDBGRanges", function(object) {
    cat(summary(object), ":\n", sep = "")
    if (length(object) > 0L) {
        m <- .makePrettyCharacterMatrixForDisplay(as(object, "DuckDBDataFrame"))
        k <- NCOL(object@elementMetadata)
        if (k > 0L) {
            nc <- ncol(m)
            h <- nc - k
            m <- cbind(m[, 1:h, drop = FALSE],
                       `|` = ifelse(rownames(m) == "...", ".", "|"),
                       m[, (h + 1L):nc, drop = FALSE])
        }
        print(m, quote = FALSE, right = TRUE)
    }
    cat("-------\n")
    cat("seqinfo: ", summary(object@seqinfo), "\n", sep = "")
})
