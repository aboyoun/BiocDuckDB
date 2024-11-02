# Tests the basic functions of a ParquetColumn.
# library(testthat); library(ParquetDataFrame); source("setup.R"); source("test-ParquetColumn.R")

test_that("Arith methods work as expected for a ParquetArray", {
    df <- ParquetDataFrame(mtcars_path, key = "model")
    mpg <- df[["mpg"]]
    disp <- df[["disp"]]

    ## "+"
    checkParquetColumn(mpg + disp, as.vector(mpg) + as.vector(disp))
    checkParquetColumn(mpg + 1L, as.vector(mpg) + 1L)
    checkParquetColumn(mpg + 3.14, as.vector(mpg) + 3.14)
    checkParquetColumn(1L + mpg, 1L + as.vector(mpg))
    checkParquetColumn(3.14 + mpg, 3.14 + as.vector(mpg))

    ## "-"
    checkParquetColumn(mpg - disp, as.vector(mpg) - as.vector(disp))
    checkParquetColumn(mpg - 1L, as.vector(mpg) - 1L)
    checkParquetColumn(mpg - 3.14, as.vector(mpg) - 3.14)
    checkParquetColumn(1L - mpg, 1L - as.vector(mpg))
    checkParquetColumn(3.14 - mpg, 3.14 - as.vector(mpg))

    ## "*"
    checkParquetColumn(mpg * disp, as.vector(mpg) * as.vector(disp))
    checkParquetColumn(mpg * 1L, as.vector(mpg) * 1L)
    checkParquetColumn(mpg * 3.14, as.vector(mpg) * 3.14)
    checkParquetColumn(1L * mpg, 1L * as.vector(mpg))
    checkParquetColumn(3.14 * mpg, 3.14 * as.vector(mpg))

    ## "/"
    checkParquetColumn(mpg / disp, as.vector(mpg) / as.vector(disp))
    checkParquetColumn(mpg / 1L, as.vector(mpg) / 1L)
    checkParquetColumn(mpg / 3.14, as.vector(mpg) / 3.14)
    checkParquetColumn(1L / mpg, 1L / as.vector(mpg))
    checkParquetColumn(3.14 / mpg, 3.14 / as.vector(mpg))

    ## "^"
    checkParquetColumn(mpg ^ disp, as.vector(mpg) ^ as.vector(disp))
    checkParquetColumn(mpg ^ 1L, as.vector(mpg) ^ 1L)
    checkParquetColumn(mpg ^ 3.14, as.vector(mpg) ^ 3.14)
    checkParquetColumn(1L ^ mpg, 1L ^ as.vector(mpg))
    checkParquetColumn(3.14 ^ mpg, 3.14 ^ as.vector(mpg))

    ## "%%"
    checkParquetColumn(mpg %% disp, as.vector(mpg) %% as.vector(disp))
    checkParquetColumn(mpg %% 1L, as.vector(mpg) %% 1L)
    checkParquetColumn(mpg %% 3.14, as.vector(mpg) %% 3.14)
    checkParquetColumn(1L %% mpg, 1L %% as.vector(mpg))
    checkParquetColumn(3.14 %% mpg, 3.14 %% as.vector(mpg))

    ## "%/%"
    checkParquetColumn(mpg %/% disp, as.vector(mpg) %/% as.vector(disp))
    checkParquetColumn(mpg %/% 1L, as.vector(mpg) %/% 1L)
    checkParquetColumn(mpg %/% 3.14, as.vector(mpg) %/% 3.14)
    checkParquetColumn(1L %/% mpg, 1L %/% as.vector(mpg))
    checkParquetColumn(3.14 %/% mpg, 3.14 %/% as.vector(mpg))
})

test_that("Compare methods work as expected for a ParquetColumn", {
    df <- ParquetDataFrame(mtcars_path, key = "model")
    vs <- df[["vs"]]
    am <- df[["am"]]

    ## "=="
    checkParquetColumn(vs == am, as.vector(vs) == as.vector(am))
    checkParquetColumn(vs == 1L, as.vector(vs) == 1L)
    checkParquetColumn(vs == 1, as.vector(vs) == 1)
    checkParquetColumn(1L == vs, 1L == as.vector(vs))
    checkParquetColumn(1 == vs, 1 == as.vector(vs))

    ## ">"
    checkParquetColumn(vs > am, as.vector(vs) > as.vector(am))
    checkParquetColumn(vs > 1L, as.vector(vs) > 1L)
    checkParquetColumn(vs > 1, as.vector(vs) > 1)
    checkParquetColumn(1L > vs, 1L > as.vector(vs))
    checkParquetColumn(1 > vs, 1 > as.vector(vs))

    ## "<"
    checkParquetColumn(vs < am, as.vector(vs) < as.vector(am))
    checkParquetColumn(vs < 1L, as.vector(vs) < 1L)
    checkParquetColumn(vs < 1, as.vector(vs) < 1)
    checkParquetColumn(1L < vs, 1L < as.vector(vs))
    checkParquetColumn(1 < vs, 1 < as.vector(vs))

    ## "!="
    checkParquetColumn(vs != am, as.vector(vs) != as.vector(am))
    checkParquetColumn(vs != 1L, as.vector(vs) != 1L)
    checkParquetColumn(vs != 1, as.vector(vs) != 1)
    checkParquetColumn(1L != vs, 1L != as.vector(vs))
    checkParquetColumn(1 != vs, 1 != as.vector(vs))

    ## "<="
    checkParquetColumn(vs <= am, as.vector(vs) <= as.vector(am))
    checkParquetColumn(vs <= 1L, as.vector(vs) <= 1L)
    checkParquetColumn(vs <= 1, as.vector(vs) <= 1)
    checkParquetColumn(1L <= vs, 1L <= as.vector(vs))
    checkParquetColumn(1 <= vs, 1 <= as.vector(vs))

    ## ">="
    checkParquetColumn(vs >= am, as.vector(vs) >= as.vector(am))
    checkParquetColumn(vs >= 1L, as.vector(vs) >= 1L)
    checkParquetColumn(vs >= 1, as.vector(vs) >= 1)
    checkParquetColumn(1L >= vs, 1L >= as.vector(vs))
    checkParquetColumn(1 >= vs, 1 >= as.vector(vs))
})

test_that("Math methods work as expected for a ParquetColumn", {
    df <- ParquetDataFrame(mtcars_path, key = "model")
    mpg <- df[["mpg"]]

    checkParquetColumn(abs(mpg), abs(as.vector(mpg)))
    checkParquetColumn(sqrt(mpg), sqrt(as.vector(mpg)))
    checkParquetColumn(ceiling(mpg), ceiling(as.vector(mpg)))
    checkParquetColumn(floor(mpg), floor(as.vector(mpg)))
    checkParquetColumn(trunc(mpg), trunc(as.vector(mpg)))

    expect_error(cummax(mpg))
    expect_error(cummin(mpg))
    expect_error(cumprod(mpg))
    expect_error(cumsum(mpg))

    checkParquetColumn(log(mpg), log(as.vector(mpg)))
    checkParquetColumn(log10(mpg), log10(as.vector(mpg)))
    checkParquetColumn(log2(mpg), log2(as.vector(mpg)))

    expect_error(log1p(mpg))
})

test_that("Summary methods work as expected for a ParquetColumn", {
    df <- ParquetDataFrame(mtcars_path, key = "model")
    mpg <- df[["mpg"]]
    am <- df[["am"]]
    expect_identical(max(mpg), max(as.vector(mpg)))
    expect_identical(min(mpg), min(as.vector(mpg)))
    expect_identical(range(mpg), range(as.vector(mpg)))
    expect_equal(prod(mpg), prod(as.vector(mpg)))
    expect_equal(sum(mpg), sum(as.vector(mpg)))
    expect_identical(any(am == 0L), any(as.vector(am) == 0L))
    expect_identical(all(am == 0L), all(as.vector(am) == 0L))
})

test_that("Other aggregate methods work as expected for a ParquetColumn", {
    df <- ParquetDataFrame(mtcars_path, key = "model")
    mpg <- df[["mpg"]]
    expect_equal(mean(mpg), mean(as.vector(mpg)))
    expect_equal(median(mpg), median(as.vector(mpg)))
    expect_equal(var(mpg), var(as.vector(mpg)))
    expect_equal(sd(mpg), sd(as.vector(mpg)))
    expect_equal(mad(mpg), mad(as.vector(mpg)))

    expect_equal(quantile(mpg), quantile(as.vector(mpg)))
    expect_equal(quantile(mpg, probs = seq(0, 1, by = 0.05)), quantile(as.vector(mpg), probs = seq(0, 1, by = 0.05)))
    expect_equal(quantile(mpg, names = FALSE), quantile(as.vector(mpg), names = FALSE))
    expect_equal(quantile(mpg, type = 1), quantile(as.vector(mpg), type = 1))
})
