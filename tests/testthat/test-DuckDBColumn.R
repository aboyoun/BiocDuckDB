# Tests the basic functions of a DuckDBColumn.
# library(testthat); library(BiocDuckDB); source("setup.R"); source("test-DuckDBColumn.R")

test_that("DuckDBColumn can be cast to a different type", {
    df <- DuckDBDataFrame(mtcars_path, key = "model")
    cyl <- df[["cyl"]]

    checkDuckDBColumn(cyl, as.vector(cyl))

    type(cyl) <- "integer"
    expected <- as.vector(cyl)
    storage.mode(expected) <- "integer"
    checkDuckDBColumn(cyl, expected)
})

test_that("nonzero functions work for DuckDBColumn", {
    df <- DuckDBDataFrame(mtcars_path, key = "model")
    am <- df[["am"]]
    checkDuckDBColumn(is_nonzero(am), is_nonzero(as.vector(am)))
    expect_equal(nzcount(am), nzcount(as.vector(am)))
})

test_that("Arith methods work as expected for a DuckDBArray", {
    df <- DuckDBDataFrame(mtcars_path, key = "model")
    mpg <- df[["mpg"]]
    disp <- df[["disp"]]

    ## "+"
    checkDuckDBColumn(mpg + disp, as.vector(mpg) + as.vector(disp))
    checkDuckDBColumn(mpg + 1L, as.vector(mpg) + 1L)
    checkDuckDBColumn(mpg + 3.14, as.vector(mpg) + 3.14)
    checkDuckDBColumn(1L + mpg, 1L + as.vector(mpg))
    checkDuckDBColumn(3.14 + mpg, 3.14 + as.vector(mpg))

    ## "-"
    checkDuckDBColumn(mpg - disp, as.vector(mpg) - as.vector(disp))
    checkDuckDBColumn(mpg - 1L, as.vector(mpg) - 1L)
    checkDuckDBColumn(mpg - 3.14, as.vector(mpg) - 3.14)
    checkDuckDBColumn(1L - mpg, 1L - as.vector(mpg))
    checkDuckDBColumn(3.14 - mpg, 3.14 - as.vector(mpg))

    ## "*"
    checkDuckDBColumn(mpg * disp, as.vector(mpg) * as.vector(disp))
    checkDuckDBColumn(mpg * 1L, as.vector(mpg) * 1L)
    checkDuckDBColumn(mpg * 3.14, as.vector(mpg) * 3.14)
    checkDuckDBColumn(1L * mpg, 1L * as.vector(mpg))
    checkDuckDBColumn(3.14 * mpg, 3.14 * as.vector(mpg))

    ## "/"
    checkDuckDBColumn(mpg / disp, as.vector(mpg) / as.vector(disp))
    checkDuckDBColumn(mpg / 1L, as.vector(mpg) / 1L)
    checkDuckDBColumn(mpg / 3.14, as.vector(mpg) / 3.14)
    checkDuckDBColumn(1L / mpg, 1L / as.vector(mpg))
    checkDuckDBColumn(3.14 / mpg, 3.14 / as.vector(mpg))

    ## "^"
    checkDuckDBColumn(mpg ^ disp, as.vector(mpg) ^ as.vector(disp))
    checkDuckDBColumn(mpg ^ 1L, as.vector(mpg) ^ 1L)
    checkDuckDBColumn(mpg ^ 3.14, as.vector(mpg) ^ 3.14)
    checkDuckDBColumn(1L ^ mpg, 1L ^ as.vector(mpg))
    checkDuckDBColumn(3.14 ^ mpg, 3.14 ^ as.vector(mpg))

    ## "%%"
    checkDuckDBColumn(mpg %% disp, as.vector(mpg) %% as.vector(disp))
    checkDuckDBColumn(mpg %% 1L, as.vector(mpg) %% 1L)
    checkDuckDBColumn(mpg %% 3.14, as.vector(mpg) %% 3.14)
    checkDuckDBColumn(1L %% mpg, 1L %% as.vector(mpg))
    checkDuckDBColumn(3.14 %% mpg, 3.14 %% as.vector(mpg))

    ## "%/%"
    checkDuckDBColumn(mpg %/% disp, as.vector(mpg) %/% as.vector(disp))
    checkDuckDBColumn(mpg %/% 1L, as.vector(mpg) %/% 1L)
    checkDuckDBColumn(mpg %/% 3.14, as.vector(mpg) %/% 3.14)
    checkDuckDBColumn(1L %/% mpg, 1L %/% as.vector(mpg))
    checkDuckDBColumn(3.14 %/% mpg, 3.14 %/% as.vector(mpg))
})

test_that("Compare methods work as expected for a DuckDBColumn", {
    df <- DuckDBDataFrame(mtcars_path, key = "model")
    vs <- df[["vs"]]
    am <- df[["am"]]

    ## "=="
    checkDuckDBColumn(vs == am, as.vector(vs) == as.vector(am))
    checkDuckDBColumn(vs == 1L, as.vector(vs) == 1L)
    checkDuckDBColumn(vs == 1, as.vector(vs) == 1)
    checkDuckDBColumn(1L == vs, 1L == as.vector(vs))
    checkDuckDBColumn(1 == vs, 1 == as.vector(vs))

    ## ">"
    checkDuckDBColumn(vs > am, as.vector(vs) > as.vector(am))
    checkDuckDBColumn(vs > 1L, as.vector(vs) > 1L)
    checkDuckDBColumn(vs > 1, as.vector(vs) > 1)
    checkDuckDBColumn(1L > vs, 1L > as.vector(vs))
    checkDuckDBColumn(1 > vs, 1 > as.vector(vs))

    ## "<"
    checkDuckDBColumn(vs < am, as.vector(vs) < as.vector(am))
    checkDuckDBColumn(vs < 1L, as.vector(vs) < 1L)
    checkDuckDBColumn(vs < 1, as.vector(vs) < 1)
    checkDuckDBColumn(1L < vs, 1L < as.vector(vs))
    checkDuckDBColumn(1 < vs, 1 < as.vector(vs))

    ## "!="
    checkDuckDBColumn(vs != am, as.vector(vs) != as.vector(am))
    checkDuckDBColumn(vs != 1L, as.vector(vs) != 1L)
    checkDuckDBColumn(vs != 1, as.vector(vs) != 1)
    checkDuckDBColumn(1L != vs, 1L != as.vector(vs))
    checkDuckDBColumn(1 != vs, 1 != as.vector(vs))

    ## "<="
    checkDuckDBColumn(vs <= am, as.vector(vs) <= as.vector(am))
    checkDuckDBColumn(vs <= 1L, as.vector(vs) <= 1L)
    checkDuckDBColumn(vs <= 1, as.vector(vs) <= 1)
    checkDuckDBColumn(1L <= vs, 1L <= as.vector(vs))
    checkDuckDBColumn(1 <= vs, 1 <= as.vector(vs))

    ## ">="
    checkDuckDBColumn(vs >= am, as.vector(vs) >= as.vector(am))
    checkDuckDBColumn(vs >= 1L, as.vector(vs) >= 1L)
    checkDuckDBColumn(vs >= 1, as.vector(vs) >= 1)
    checkDuckDBColumn(1L >= vs, 1L >= as.vector(vs))
    checkDuckDBColumn(1 >= vs, 1 >= as.vector(vs))
})

test_that("Math methods work as expected for a DuckDBColumn", {
    df <- DuckDBDataFrame(mtcars_path, key = "model")
    mpg <- df[["mpg"]]

    checkDuckDBColumn(abs(mpg), abs(as.vector(mpg)))
    checkDuckDBColumn(sqrt(mpg), sqrt(as.vector(mpg)))
    checkDuckDBColumn(ceiling(mpg), ceiling(as.vector(mpg)))
    checkDuckDBColumn(floor(mpg), floor(as.vector(mpg)))
    checkDuckDBColumn(trunc(mpg), trunc(as.vector(mpg)))

    expect_error(cummax(mpg))
    expect_error(cummin(mpg))
    expect_error(cumprod(mpg))
    expect_error(cumsum(mpg))

    checkDuckDBColumn(log(mpg), log(as.vector(mpg)))
    checkDuckDBColumn(log10(mpg), log10(as.vector(mpg)))
    checkDuckDBColumn(log2(mpg), log2(as.vector(mpg)))

    expect_error(log1p(mpg))
})

test_that("Summary methods work as expected for a DuckDBColumn", {
    df <- DuckDBDataFrame(mtcars_path, key = "model")
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

test_that("Other aggregate methods work as expected for a DuckDBColumn", {
    df <- DuckDBDataFrame(mtcars_path, key = "model")
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
