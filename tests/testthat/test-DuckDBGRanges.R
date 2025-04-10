# Tests the basic functions of a DuckDBGRanges.
# library(testthat); library(BiocDuckDB); source("setup.R"); source("test-DuckDBGRanges.R")

library(GenomicRanges)

test_that("DuckDBGRanges constructor works as expected", {
    seqinfo <- Seqinfo(paste0("chr", 1:3), c(1000, 2000, 1500), NA, "mock1")

    # start only
    expected <- GRanges(granges_df[["seqnames"]], ranges = IRanges(granges_df[["start"]]))
    object <- DuckDBGRanges(granges_tf, seqnames = "seqnames", start = "start")
    checkDuckDBGRanges(object, expected)

    # start and end
    expected <- GRanges(granges_df[["seqnames"]], ranges = IRanges(start = granges_df[["start"]], end = granges_df[["end"]], names = granges_df[["id"]]))
    object <- DuckDBGRanges(granges_tf, seqnames = "seqnames", start = "start", end = "end", keycol = "id")
    checkDuckDBGRanges(object, expected)

    # start and end with mcols
    expected <- GRanges(granges_df[["seqnames"]], ranges = IRanges(start = granges_df[["start"]], end = granges_df[["end"]], names = granges_df[["id"]]),
                        score = granges_df[["score"]], GC = granges_df[["GC"]])
    object <- DuckDBGRanges(granges_tf, seqnames = "seqnames", start = "start", end = "end", mcols = c("score", "GC"), keycol = "id")
    checkDuckDBGRanges(object, expected)

    # start and width
    expected <- GRanges(granges_df[["seqnames"]], ranges = IRanges(start = granges_df[["start"]], width = granges_df[["width"]], names = granges_df[["id"]]))
    object <- DuckDBGRanges(granges_tf, seqnames = "seqnames", start = "start", width = "width", keycol = "id")
    checkDuckDBGRanges(object, expected)

    # start and width with mcols
    expected <- GRanges(granges_df[["seqnames"]], ranges = IRanges(start = granges_df[["start"]], width = granges_df[["width"]], names = granges_df[["id"]]),
                        score = granges_df[["score"]], GC = granges_df[["GC"]])
    object <- DuckDBGRanges(granges_tf, seqnames = "seqnames", start = "start", width = "width", mcols = c("score", "GC"), keycol = "id")
    checkDuckDBGRanges(object, expected)

    # end and width
    expected <- GRanges(granges_df[["seqnames"]], ranges = IRanges(end = granges_df[["end"]], width = granges_df[["width"]], names = granges_df[["id"]]))
    object <- DuckDBGRanges(granges_tf, seqnames = "seqnames", end = "end", width = "width", keycol = "id")
    checkDuckDBGRanges(object, expected)

    # end and width with mcols
    expected <- GRanges(granges_df[["seqnames"]], ranges = IRanges(end = granges_df[["end"]], width = granges_df[["width"]], names = granges_df[["id"]]),
                        score = granges_df[["score"]], GC = granges_df[["GC"]])
    object <- DuckDBGRanges(granges_tf, seqnames = "seqnames", end = "end", width = "width", mcols = c("score", "GC"), keycol = "id")
    checkDuckDBGRanges(object, expected)
})

test_that("coersion to a GRanges works for a DuckDBGRanges", {
    seqinfo <- Seqinfo(paste0("chr", 1:3), c(1000, 2000, 1500), NA, "mock1")

    # seqinfo
    expected <- GRanges(granges_df[["seqnames"]],
                        ranges = IRanges(start = granges_df[["start"]], width = granges_df[["width"]], names = granges_df[["id"]]),
                        seqinfo = seqinfo)
    object <- DuckDBGRanges(granges_tf, seqnames = "seqnames", start = "start", width = "width",
                            keycol = list(id = granges_df[["id"]]), seqinfo = seqinfo)
    expect_identical(as(object, "GRanges"), expected)

    # strand and seqinfo
    expected <- GRanges(granges_df[["seqnames"]],
                        ranges = IRanges(start = granges_df[["start"]], width = granges_df[["width"]], names = granges_df[["id"]]),
                        strand = granges_df[["strand"]], seqinfo = seqinfo)
    object <- DuckDBGRanges(granges_tf, seqnames = "seqnames", start = "start", width = "width", strand = "strand",
                            keycol = list(id = granges_df[["id"]]), seqinfo = seqinfo)
    expect_identical(as(object, "GRanges"), expected)

    # strand, mcols, and seqinfo
    expected <- GRanges(granges_df[["seqnames"]],
                        ranges = IRanges(start = granges_df[["start"]], width = granges_df[["width"]], names = granges_df[["id"]]),
                        strand = granges_df[["strand"]], score = granges_df[["score"]], GC = granges_df[["GC"]], seqinfo = seqinfo)
    object <- DuckDBGRanges(granges_tf, seqnames = "seqnames", start = "start", width = "width", strand = "strand",
                            mcols = c("score", "GC"), keycol = list(id = granges_df[["id"]]), seqinfo = seqinfo)
    expect_identical(as(object, "GRanges"), expected)
})
