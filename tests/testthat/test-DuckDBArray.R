# Tests the basic functions of a DuckDBArray.
# library(testthat); library(BiocDuckDB); source("setup.R"); source("test-DuckDBArray.R")

test_that("basic methods work as expected for a DuckDBArray", {
    pqarray <- DuckDBArray(titanic_path, keycols = dimnames(titanic_array), datacols = "fate")
    checkDuckDBArray(pqarray, titanic_array)
    expect_false(is_sparse(pqarray))

    pqarray <- DuckDBArray(titanic_path, keycols = dimnames(titanic_array), datacols = "fate", type = "double")
    expect_s4_class(pqarray, "DuckDBArray")
    expect_identical(type(pqarray), "double")
    expect_identical(length(pqarray), length(titanic_array))
    expect_identical(dim(pqarray), dim(titanic_array))
    expect_identical(dimnames(pqarray), dimnames(titanic_array))
    expect_equal(as.array(pqarray), titanic_array)

    pqarray <- DuckDBArray(titanic_path, keycols = dimnames(titanic_array), datacols = "fate", type = "character")
    expect_s4_class(pqarray, "DuckDBArray")
    expect_identical(type(pqarray), "character")
    expect_identical(length(pqarray), length(titanic_array))
    expect_identical(dim(pqarray), dim(titanic_array))
    expect_identical(dimnames(pqarray), dimnames(titanic_array))
})

test_that("basic methods work as expected for a sparse DuckDBArray", {
    pqarray <- DuckDBArray(sparse_path, keycols = list(dim1 = LETTERS, dim2 = letters, dim3 = month.abb), datacols = "value")
    checkDuckDBArray(pqarray, sparse_array)
    expect_true(is_sparse(pqarray))

    pqarray <- DuckDBArray(sparse_path, keycols = list(dim1 = LETTERS, dim2 = letters, dim3 = month.abb), datacols = "value", type = "double")
    expect_s4_class(pqarray, "DuckDBArray")
    expect_identical(type(pqarray), "double")
    expect_identical(length(pqarray), length(sparse_array))
    expect_identical(dim(pqarray), dim(sparse_array))
    expect_identical(dimnames(pqarray), dimnames(sparse_array))
    expect_equal(as.array(pqarray), sparse_array)

    pqarray <- DuckDBArray(sparse_path, keycols = list(dim1 = LETTERS, dim2 = letters, dim3 = month.abb), datacols = "value", type = "character")
    expect_s4_class(pqarray, "DuckDBArray")
    expect_identical(type(pqarray), "character")
    expect_identical(length(pqarray), length(sparse_array))
    expect_identical(dim(pqarray), dim(sparse_array))
    expect_identical(dimnames(pqarray), dimnames(sparse_array))
})

test_that("DuckDBArray can be cast to a different type", {
    pqarray <- DuckDBArray(titanic_path, keycols = dimnames(titanic_array), datacols = "fate")
    type(pqarray) <- "double"
    expected <- titanic_array
    storage.mode(expected) <- "double"
    checkDuckDBArray(pqarray, expected)

    pqarray <- DuckDBArray(sparse_path, keycols = list(dim1 = LETTERS, dim2 = letters, dim3 = month.abb), datacols = "value")
    type(pqarray) <- "double"
    expected <- sparse_array
    storage.mode(expected) <- "double"
    checkDuckDBArray(pqarray, expected)
})

test_that("nonzero functions work for DuckDBArray", {
    pqarray <- DuckDBArray(titanic_path, keycols = dimnames(titanic_array), datacols = "fate")
    checkDuckDBArray(is_nonzero(pqarray), is_nonzero(titanic_array))
    expect_equal(nzcount(pqarray), nzcount(titanic_array))

    pqarray <- DuckDBArray(sparse_path, keycols = list(dim1 = LETTERS, dim2 = letters, dim3 = month.abb), datacols = "value")
    checkDuckDBArray(is_nonzero(pqarray), is_nonzero(sparse_array))
    expect_equal(nzcount(pqarray), nzcount(sparse_array))
})

test_that("extraction methods work as expected for a DuckDBArray", {
    pqarray <- DuckDBArray(titanic_path, keycols = dimnames(titanic_array), datacols = "fate")

    expect_error(pqarray[,])

    object <- pqarray[]
    checkDuckDBArray(object, titanic_array)

    object <- pqarray[, 2:1, , ]
    expected <- titanic_array[, 2:1, , ]
    checkDuckDBArray(object, expected)

    object <- pqarray[c(4, 2), , 1, ]
    expected <- titanic_array[c(4, 2), , 1, ]
    checkDuckDBArray(object, expected)

    object <- pqarray[c(4, 2), , 1, , drop = FALSE]
    expected <- titanic_array[c(4, 2), , 1, , drop = FALSE]
    checkDuckDBArray(object, expected)

    object <- pqarray[4, 2, 1, 2]
    expected <- as.array(titanic_array[4, 2, 1, 2])
    checkDuckDBArray(object, expected)

    object <- pqarray[4, 2, 1, 2, drop = FALSE]
    expected <- titanic_array[4, 2, 1, 2, drop = FALSE]
    checkDuckDBArray(object, expected)

    object <- pqarray[c("1st", "2nd", "3rd"), "Female", "Child", ]
    expected <- titanic_array[c("1st", "2nd", "3rd"), "Female", "Child", ]
    checkDuckDBArray(object, expected)
})

test_that("aperm and t methods work as expected for a DuckDBArray", {
    seed <- DuckDBArray(titanic_path, keycols = dimnames(titanic_array), datacols = "fate")

    object <- aperm(seed, c(4, 2, 1, 3))
    expected <- aperm(titanic_array, c(4, 2, 1, 3))
    checkDuckDBArray(object, expected)

    names(dimnames(state.x77)) <- c("rowname", "colname")
    seed <- DuckDBArray(state_path, keycols = dimnames(state.x77), datacols = "value")

    object <- t(seed)
    expected <- t(state.x77)
    expect_s4_class(object, "DuckDBArray")
    expect_identical(type(object), "double")
    expect_identical(length(object), length(expected))
    expect_identical(dim(object), dim(expected))
    expect_identical(dimnames(object)[[1L]], dimnames(expected)[[1L]])
    expect_setequal(dimnames(object)[[2L]], dimnames(expected)[[2L]])
    expect_identical(as.array(object)[, colnames(expected)], expected)
})

test_that("Arith methods work as expected for a DuckDBArray", {
    pqarray <- DuckDBArray(titanic_path, keycols = dimnames(titanic_array), datacols = "fate")

    ## "+"
    checkDuckDBArray(pqarray + sqrt(pqarray), as.array(pqarray) + sqrt(as.array(pqarray)))
    checkDuckDBArray(pqarray + 1L, as.array(pqarray) + 1L)
    checkDuckDBArray(pqarray + 3.14, as.array(pqarray) + 3.14)
    checkDuckDBArray(1L + pqarray, 1L + as.array(pqarray))
    checkDuckDBArray(3.14 + pqarray, 3.14 + as.array(pqarray))

    ## "-"
    checkDuckDBArray(pqarray - sqrt(pqarray), as.array(pqarray) - sqrt(as.array(pqarray)))
    checkDuckDBArray(pqarray - 1L, as.array(pqarray) - 1L)
    checkDuckDBArray(pqarray - 3.14, as.array(pqarray) - 3.14)
    checkDuckDBArray(1L - pqarray, 1L - as.array(pqarray))
    checkDuckDBArray(3.14 - pqarray, 3.14 - as.array(pqarray))

    ## "*"
    checkDuckDBArray(pqarray * sqrt(pqarray), as.array(pqarray) * sqrt(as.array(pqarray)))
    checkDuckDBArray(pqarray * 1L, as.array(pqarray) * 1L)
    checkDuckDBArray(pqarray * 3.14, as.array(pqarray) * 3.14)
    checkDuckDBArray(1L * pqarray, 1L * as.array(pqarray))
    checkDuckDBArray(3.14 * pqarray, 3.14 * as.array(pqarray))

    ## "/"
    checkDuckDBArray(pqarray / sqrt(pqarray), as.array(pqarray) / sqrt(as.array(pqarray)))
    checkDuckDBArray(pqarray / 1L, as.array(pqarray) / 1L)
    checkDuckDBArray(pqarray / 3.14, as.array(pqarray) / 3.14)
    checkDuckDBArray(1L / pqarray, 1L / as.array(pqarray))
    checkDuckDBArray(3.14 / pqarray, 3.14 / as.array(pqarray))

    ## "^"
    checkDuckDBArray(pqarray ^ sqrt(pqarray), as.array(pqarray) ^ sqrt(as.array(pqarray)))
    checkDuckDBArray(pqarray ^ 3.14, as.array(pqarray) ^ 3.14)
    checkDuckDBArray(3.14 ^ pqarray, 3.14 ^ as.array(pqarray))

    ## "%%"
    checkDuckDBArray(pqarray %% sqrt(pqarray), as.array(pqarray) %% sqrt(as.array(pqarray)))
    checkDuckDBArray(pqarray %% 3.14, as.array(pqarray) %% 3.14)
    checkDuckDBArray(3.14 %% pqarray, 3.14 %% as.array(pqarray))

    ## "%/%"
    checkDuckDBArray(pqarray %/% sqrt(pqarray), as.array(pqarray) %/% sqrt(as.array(pqarray)))
    checkDuckDBArray(pqarray %/% 3.14, as.array(pqarray) %/% 3.14)
    checkDuckDBArray(3.14 %/% pqarray, 3.14 %/% as.array(pqarray))
})

test_that("Compare methods work as expected for a DuckDBArray", {
    pqarray <- DuckDBArray(titanic_path, keycols = dimnames(titanic_array), datacols = "fate")

    ## "=="
    checkDuckDBArray(pqarray == sqrt(pqarray), as.array(pqarray) == sqrt(as.array(pqarray)))
    checkDuckDBArray(pqarray == 1L, as.array(pqarray) == 1L)
    checkDuckDBArray(pqarray == 3.14, as.array(pqarray) == 3.14)
    checkDuckDBArray(1L == pqarray, 1L == as.array(pqarray))
    checkDuckDBArray(3.14 == pqarray, 3.14 == as.array(pqarray))

    ## ">"
    checkDuckDBArray(pqarray > sqrt(pqarray), as.array(pqarray) > sqrt(as.array(pqarray)))
    checkDuckDBArray(pqarray > 1L, as.array(pqarray) > 1L)
    checkDuckDBArray(pqarray > 3.14, as.array(pqarray) > 3.14)
    checkDuckDBArray(1L > pqarray, 1L > as.array(pqarray))
    checkDuckDBArray(3.14 > pqarray, 3.14 > as.array(pqarray))

    ## "<"
    checkDuckDBArray(pqarray < sqrt(pqarray), as.array(pqarray) < sqrt(as.array(pqarray)))
    checkDuckDBArray(pqarray < 1L, as.array(pqarray) < 1L)
    checkDuckDBArray(pqarray < 3.14, as.array(pqarray) < 3.14)
    checkDuckDBArray(1L < pqarray, 1L < as.array(pqarray))
    checkDuckDBArray(3.14 < pqarray, 3.14 < as.array(pqarray))

    ## "!="
    checkDuckDBArray(pqarray != sqrt(pqarray), as.array(pqarray) != sqrt(as.array(pqarray)))
    checkDuckDBArray(pqarray != 1L, as.array(pqarray) != 1L)
    checkDuckDBArray(pqarray != 3.14, as.array(pqarray) != 3.14)
    checkDuckDBArray(1L != pqarray, 1L != as.array(pqarray))
    checkDuckDBArray(3.14 != pqarray, 3.14 != as.array(pqarray))

    ## "<="
    checkDuckDBArray(pqarray <= sqrt(pqarray), as.array(pqarray) <= sqrt(as.array(pqarray)))
    checkDuckDBArray(pqarray <= 1L, as.array(pqarray) <= 1L)
    checkDuckDBArray(pqarray <= 3.14, as.array(pqarray) <= 3.14)
    checkDuckDBArray(1L <= pqarray, 1L <= as.array(pqarray))
    checkDuckDBArray(3.14 <= pqarray, 3.14 <= as.array(pqarray))

    ## ">="
    checkDuckDBArray(pqarray >= sqrt(pqarray), as.array(pqarray) >= sqrt(as.array(pqarray)))
    checkDuckDBArray(pqarray >= 1L, as.array(pqarray) >= 1L)
    checkDuckDBArray(pqarray >= 3.14, as.array(pqarray) >= 3.14)
    checkDuckDBArray(1L >= pqarray, 1L >= as.array(pqarray))
    checkDuckDBArray(3.14 >= pqarray, 3.14 >= as.array(pqarray))
})

test_that("Logic methods work as expected for a DuckDBArray", {
    pqarray <- DuckDBArray(titanic_path, keycols = dimnames(titanic_array), datacols = "fate")

    ## "&"
    x <- pqarray > 70
    y <- pqarray < 4000
    checkDuckDBArray(x & y, as.array(x) & as.array(y))

    ## "|"
    x <- pqarray > 70
    y <- sqrt(pqarray) > 0
    checkDuckDBArray(x | y, as.array(x) | as.array(y))
})

test_that("Math methods work as expected for a DuckDBArray", {
    names(dimnames(state.x77)) <- c("rowname", "colname")
    pqarray <- DuckDBArray(state_path, keycols = dimnames(state.x77), datacols = "value")

    income <- pqarray[, "Income"]
    ikeep <-
      c("Colorado", "Delaware", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
        "Maine", "Maryland", "Michigan", "Minnesota", "Missouri", "Montana",
        "Nebraska", "Nevada", "New Hampshire", "North Dakota", "Ohio", "Oregon",
        "Pennsylvania", "South Dakota", "Utah", "Vermont", "Washington", "Wisconsin",
        "Wyoming")
    illiteracy <- pqarray[ikeep, "Illiteracy"]

    checkDuckDBArray(abs(income), abs(as.array(income)))
    checkDuckDBArray(sqrt(income), sqrt(as.array(income)))
    checkDuckDBArray(ceiling(income), ceiling(as.array(income)))
    checkDuckDBArray(floor(income), floor(as.array(income)))
    checkDuckDBArray(trunc(income), trunc(as.array(income)))

    expect_error(cummax(income))
    expect_error(cummin(income))
    expect_error(cumprod(income))
    expect_error(cumsum(income))

    checkDuckDBArray(log(income), log(as.array(income)))
    checkDuckDBArray(log10(income), log10(as.array(income)))
    checkDuckDBArray(log2(income), log2(as.array(income)))

    expect_error(log1p(income))

    checkDuckDBArray(acos(illiteracy), acos(as.array(illiteracy)))
    checkDuckDBArray(acosh(income), acosh(as.array(income)))
    checkDuckDBArray(asin(illiteracy), asin(as.array(illiteracy)))
    checkDuckDBArray(asinh(income), asinh(as.array(income)))
    checkDuckDBArray(atan(income), atan(as.array(income)))
    checkDuckDBArray(atanh(illiteracy), atanh(as.array(illiteracy)))

    checkDuckDBArray(exp(income), exp(as.array(income)))

    expect_error(expm1(income))

    checkDuckDBArray(cos(illiteracy), cos(as.array(illiteracy)))
    checkDuckDBArray(cosh(illiteracy), cosh(as.array(illiteracy)))

    expect_error(cospi(illiteracy))

    checkDuckDBArray(sin(illiteracy), sin(as.array(illiteracy)))
    checkDuckDBArray(sinh(illiteracy), sinh(as.array(illiteracy)))

    expect_error(sinpi(illiteracy))

    checkDuckDBArray(tan(illiteracy), tan(as.array(illiteracy)))
    checkDuckDBArray(tanh(illiteracy), tanh(as.array(illiteracy)))

    expect_error(tanpi(illiteracy))

    checkDuckDBArray(gamma(illiteracy), gamma(as.array(illiteracy)))
    checkDuckDBArray(lgamma(illiteracy), lgamma(as.array(illiteracy)))

    expect_error(digamma(illiteracy))
    expect_error(trigamma(illiteracy))
})

test_that("Summary methods work as expected for a DuckDBArray", {
    pqarray <- DuckDBArray(titanic_path, keycols = dimnames(titanic_array), datacols = "fate")
    expect_identical(max(pqarray), max(as.array(pqarray)))
    expect_identical(min(pqarray), min(as.array(pqarray)))
    expect_identical(range(pqarray), range(as.array(pqarray)))
    expect_equal(prod(pqarray), prod(as.array(pqarray)))
    expect_equal(sum(pqarray), sum(as.array(pqarray)))
    expect_identical(any(pqarray == 0L), any(as.array(pqarray) == 0L))
    expect_identical(all(pqarray == 0L), all(as.array(pqarray) == 0L))
})

test_that("Other aggregate methods work as expected for a DuckDBArray", {
    pqarray <- DuckDBArray(titanic_path, keycols = dimnames(titanic_array), datacols = "fate")
    expect_equal(mean(pqarray), mean(as.array(pqarray)))
    expect_equal(median(pqarray), median(as.array(pqarray)))
    expect_equal(var(pqarray), var(as.array(pqarray)))
    expect_equal(sd(pqarray), sd(as.array(pqarray)))
    expect_equal(mad(pqarray), mad(as.array(pqarray)))

    expect_equal(quantile(pqarray), quantile(as.array(pqarray)))
    expect_equal(quantile(pqarray, probs = seq(0, 1, by = 0.05)), quantile(as.array(pqarray), probs = seq(0, 1, by = 0.05)))
    expect_equal(quantile(pqarray, names = FALSE), quantile(as.array(pqarray), names = FALSE))
    expect_equal(quantile(pqarray, type = 1), quantile(as.array(pqarray), type = 1))
})
