# Tests the basic functions of a DuckDBTransposedDataFrame.
# library(testthat); library(BiocDuckDB); source("setup.R"); source("test-DuckDBTransposedDataFrame.R")

test_that("basic methods work for a DuckDBTransposedDataFrame", {
    tdf <- t(DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = "model"))
    checkDuckDBTransposedDataFrame(tdf, mtcars)

    tdf <- t(DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = list(model = rownames(mtcars))))
    checkDuckDBTransposedDataFrame(tdf, mtcars)
    expect_identical(colnames(tdf), rownames(mtcars))
    expect_identical(as.data.frame(t(tdf)), mtcars)
    expect_identical(as.matrix(tdf), t(as.matrix(mtcars)))
    expect_identical(realize(tdf), t(realize(t(tdf))))

    tdf <- t(DuckDBDataFrame(infert_parquet))
    checkDuckDBTransposedDataFrame(tdf, infert)

    tdf <- t(DuckDBDataFrame(infert_parquet, datacols = colnames(infert)))
    checkDuckDBTransposedDataFrame(tdf, infert)
})

test_that("slicing by columns works for a DuckDBTransposedDataFrame", {
    tdf <- t(DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = list(model = rownames(mtcars))))

    keep <- 1:2
    checkDuckDBTransposedDataFrame(tdf[keep,], mtcars[,keep])

    keep <- rownames(tdf)[c(4,2,3)]
    checkDuckDBTransposedDataFrame(tdf[keep,], mtcars[,keep])

    keep <- startsWith(rownames(tdf), "d")
    checkDuckDBTransposedDataFrame(tdf[keep,], mtcars[,keep])

    keep <- 5
    checkDuckDBTransposedDataFrame(tdf[keep, , drop=FALSE], mtcars[,keep, drop=FALSE])
})
