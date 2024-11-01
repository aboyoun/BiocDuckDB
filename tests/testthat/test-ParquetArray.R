# Tests the basic functions of a ParquetArray.
# library(testthat); library(ParquetDataFrame); source("setup.R"); source("test-ParquetArray.R")

test_that("basic methods work as expected for a ParquetArray", {
    pqarray <- ParquetArray(titanic_path, key = dimnames(titanic_array), fact = "fate")
    checkParquetArray(pqarray, titanic_array)

    pqarray <- ParquetArray(titanic_path, key = dimnames(titanic_array), fact = "fate", type = "double")
    expect_s4_class(pqarray, "ParquetArray")
    expect_identical(type(pqarray), "double")
    expect_identical(length(pqarray), length(titanic_array))
    expect_identical(dim(pqarray), dim(titanic_array))
    expect_identical(dimnames(pqarray), dimnames(titanic_array))
    expect_equal(as.array(pqarray), titanic_array)

    pqarray <- ParquetArray(titanic_path, key = dimnames(titanic_array), fact = "fate", type = "character")
    expect_s4_class(pqarray, "ParquetArray")
    expect_identical(type(pqarray), "character")
    expect_identical(length(pqarray), length(titanic_array))
    expect_identical(dim(pqarray), dim(titanic_array))
    expect_identical(dimnames(pqarray), dimnames(titanic_array))
})

test_that("extraction methods work as expected for a ParquetArray", {
    pqarray <- ParquetArray(titanic_path, key = dimnames(titanic_array), fact = "fate")

    expect_error(pqarray[,])

    object <- pqarray[]
    checkParquetArray(object, titanic_array)

    object <- pqarray[, 2:1, , ]
    expected <- titanic_array[, 2:1, , ]
    checkParquetArray(object, expected)

    object <- pqarray[c(4, 2), , 1, ]
    expected <- titanic_array[c(4, 2), , 1, ]
    checkParquetArray(object, expected)

    object <- pqarray[c(4, 2), , 1, , drop = FALSE]
    expected <- titanic_array[c(4, 2), , 1, , drop = FALSE]
    checkParquetArray(object, expected)

    object <- pqarray[4, 2, 1, 2]
    expected <- as.array(titanic_array[4, 2, 1, 2])
    checkParquetArray(object, expected)

    object <- pqarray[4, 2, 1, 2, drop = FALSE]
    expected <- titanic_array[4, 2, 1, 2, drop = FALSE]
    checkParquetArray(object, expected)

    object <- pqarray[c("1st", "2nd", "3rd"), "Female", "Child", ]
    expected <- titanic_array[c("1st", "2nd", "3rd"), "Female", "Child", ]
    checkParquetArray(object, expected)
})

test_that("aperm and t methods work as expected for a ParquetArray", {
    seed <- ParquetArray(titanic_path, key = dimnames(titanic_array), fact = "fate")

    object <- aperm(seed, c(4, 2, 1, 3))
    expected <- aperm(titanic_array, c(4, 2, 1, 3))
    checkParquetArray(object, expected)

    names(dimnames(state.x77)) <- c("rowname", "colname")
    seed <- ParquetArray(state_path, key = dimnames(state.x77), fact = "value")

    object <- t(seed)
    expected <- t(state.x77)
    expect_s4_class(object, "ParquetArray")
    expect_identical(type(object), "double")
    expect_identical(length(object), length(expected))
    expect_identical(dim(object), dim(expected))
    expect_identical(dimnames(object)[[1L]], dimnames(expected)[[1L]])
    expect_setequal(dimnames(object)[[2L]], dimnames(expected)[[2L]])
    expect_identical(as.array(object)[, colnames(expected)], expected)
})

test_that("Arith methods work as expected for a ParquetArray", {
    pqarray <- ParquetArray(titanic_path, key = dimnames(titanic_array), fact = "fate")

    ## "+"
    checkParquetArray(pqarray + sqrt(pqarray), as.array(pqarray) + sqrt(as.array(pqarray)))
    checkParquetArray(pqarray + 1L, as.array(pqarray) + 1L)
    checkParquetArray(pqarray + 3.14, as.array(pqarray) + 3.14)
    checkParquetArray(1L + pqarray, 1L + as.array(pqarray))
    checkParquetArray(3.14 + pqarray, 3.14 + as.array(pqarray))

    ## "-"
    checkParquetArray(pqarray - sqrt(pqarray), as.array(pqarray) - sqrt(as.array(pqarray)))
    checkParquetArray(pqarray - 1L, as.array(pqarray) - 1L)
    checkParquetArray(pqarray - 3.14, as.array(pqarray) - 3.14)
    checkParquetArray(1L - pqarray, 1L - as.array(pqarray))
    checkParquetArray(3.14 - pqarray, 3.14 - as.array(pqarray))

    ## "*"
    checkParquetArray(pqarray * sqrt(pqarray), as.array(pqarray) * sqrt(as.array(pqarray)))
    checkParquetArray(pqarray * 1L, as.array(pqarray) * 1L)
    checkParquetArray(pqarray * 3.14, as.array(pqarray) * 3.14)
    checkParquetArray(1L * pqarray, 1L * as.array(pqarray))
    checkParquetArray(3.14 * pqarray, 3.14 * as.array(pqarray))

    ## "/"
    checkParquetArray(pqarray / sqrt(pqarray), as.array(pqarray) / sqrt(as.array(pqarray)))
    checkParquetArray(pqarray / 1L, as.array(pqarray) / 1L)
    checkParquetArray(pqarray / 3.14, as.array(pqarray) / 3.14)
    checkParquetArray(1L / pqarray, 1L / as.array(pqarray))
    checkParquetArray(3.14 / pqarray, 3.14 / as.array(pqarray))

    ## "^"
    checkParquetArray(pqarray ^ sqrt(pqarray), as.array(pqarray) ^ sqrt(as.array(pqarray)))
    checkParquetArray(pqarray ^ 3.14, as.array(pqarray) ^ 3.14)
    checkParquetArray(3.14 ^ pqarray, 3.14 ^ as.array(pqarray))

    ## "%%"
    checkParquetArray(pqarray %% sqrt(pqarray), as.array(pqarray) %% sqrt(as.array(pqarray)))
    checkParquetArray(pqarray %% 3.14, as.array(pqarray) %% 3.14)
    checkParquetArray(3.14 %% pqarray, 3.14 %% as.array(pqarray))

    ## "%/%"
    checkParquetArray(pqarray %/% sqrt(pqarray), as.array(pqarray) %/% sqrt(as.array(pqarray)))
    checkParquetArray(pqarray %/% 3.14, as.array(pqarray) %/% 3.14)
    checkParquetArray(3.14 %/% pqarray, 3.14 %/% as.array(pqarray))
})

test_that("Compare methods work as expected for a ParquetArray", {
    pqarray <- ParquetArray(titanic_path, key = dimnames(titanic_array), fact = "fate")

    ## "=="
    checkParquetArray(pqarray == sqrt(pqarray), as.array(pqarray) == sqrt(as.array(pqarray)))
    checkParquetArray(pqarray == 1L, as.array(pqarray) == 1L)
    checkParquetArray(pqarray == 3.14, as.array(pqarray) == 3.14)
    checkParquetArray(1L == pqarray, 1L == as.array(pqarray))
    checkParquetArray(3.14 == pqarray, 3.14 == as.array(pqarray))

    ## ">"
    checkParquetArray(pqarray > sqrt(pqarray), as.array(pqarray) > sqrt(as.array(pqarray)))
    checkParquetArray(pqarray > 1L, as.array(pqarray) > 1L)
    checkParquetArray(pqarray > 3.14, as.array(pqarray) > 3.14)
    checkParquetArray(1L > pqarray, 1L > as.array(pqarray))
    checkParquetArray(3.14 > pqarray, 3.14 > as.array(pqarray))

    ## "<"
    checkParquetArray(pqarray < sqrt(pqarray), as.array(pqarray) < sqrt(as.array(pqarray)))
    checkParquetArray(pqarray < 1L, as.array(pqarray) < 1L)
    checkParquetArray(pqarray < 3.14, as.array(pqarray) < 3.14)
    checkParquetArray(1L < pqarray, 1L < as.array(pqarray))
    checkParquetArray(3.14 < pqarray, 3.14 < as.array(pqarray))

    ## "!="
    checkParquetArray(pqarray != sqrt(pqarray), as.array(pqarray) != sqrt(as.array(pqarray)))
    checkParquetArray(pqarray != 1L, as.array(pqarray) != 1L)
    checkParquetArray(pqarray != 3.14, as.array(pqarray) != 3.14)
    checkParquetArray(1L != pqarray, 1L != as.array(pqarray))
    checkParquetArray(3.14 != pqarray, 3.14 != as.array(pqarray))

    ## "<="
    checkParquetArray(pqarray <= sqrt(pqarray), as.array(pqarray) <= sqrt(as.array(pqarray)))
    checkParquetArray(pqarray <= 1L, as.array(pqarray) <= 1L)
    checkParquetArray(pqarray <= 3.14, as.array(pqarray) <= 3.14)
    checkParquetArray(1L <= pqarray, 1L <= as.array(pqarray))
    checkParquetArray(3.14 <= pqarray, 3.14 <= as.array(pqarray))

    ## ">="
    checkParquetArray(pqarray >= sqrt(pqarray), as.array(pqarray) >= sqrt(as.array(pqarray)))
    checkParquetArray(pqarray >= 1L, as.array(pqarray) >= 1L)
    checkParquetArray(pqarray >= 3.14, as.array(pqarray) >= 3.14)
    checkParquetArray(1L >= pqarray, 1L >= as.array(pqarray))
    checkParquetArray(3.14 >= pqarray, 3.14 >= as.array(pqarray))
})

test_that("Logic methods work as expected for a ParquetArray", {
    pqarray <- ParquetArray(titanic_path, key = dimnames(titanic_array), fact = "fate")

    ## "&"
    x <- pqarray > 70
    y <- pqarray < 4000
    checkParquetArray(x & y, as.array(x) & as.array(y))

    ## "|"
    x <- pqarray > 70
    y <- sqrt(pqarray) > 0
    checkParquetArray(x | y, as.array(x) | as.array(y))
})

test_that("Math methods work as expected for a ParquetArray", {
    names(dimnames(state.x77)) <- c("rowname", "colname")
    pqarray <- ParquetArray(state_path, key = dimnames(state.x77), fact = "value")

    income <- pqarray[, "Income"]
    ikeep <-
      c("Colorado", "Delaware", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
        "Maine", "Maryland", "Michigan", "Minnesota", "Missouri", "Montana",
        "Nebraska", "Nevada", "New Hampshire", "North Dakota", "Ohio", "Oregon",
        "Pennsylvania", "South Dakota", "Utah", "Vermont", "Washington", "Wisconsin",
        "Wyoming")
    illiteracy <- pqarray[ikeep, "Illiteracy"]

    checkParquetArray(abs(income), abs(as.array(income)))
    checkParquetArray(sqrt(income), sqrt(as.array(income)))
    checkParquetArray(ceiling(income), ceiling(as.array(income)))
    checkParquetArray(floor(income), floor(as.array(income)))
    checkParquetArray(trunc(income), trunc(as.array(income)))

    expect_error(cummax(income))
    expect_error(cummin(income))
    expect_error(cumprod(income))
    expect_error(cumsum(income))

    checkParquetArray(log(income), log(as.array(income)))
    checkParquetArray(log10(income), log10(as.array(income)))
    checkParquetArray(log2(income), log2(as.array(income)))

    expect_error(log1p(income))

    checkParquetArray(acos(illiteracy), acos(as.array(illiteracy)))
    checkParquetArray(acosh(income), acosh(as.array(income)))
    checkParquetArray(asin(illiteracy), asin(as.array(illiteracy)))
    checkParquetArray(asinh(income), asinh(as.array(income)))
    checkParquetArray(atan(income), atan(as.array(income)))
    checkParquetArray(atanh(illiteracy), atanh(as.array(illiteracy)))

    checkParquetArray(exp(income), exp(as.array(income)))

    expect_error(expm1(income))

    checkParquetArray(cos(illiteracy), cos(as.array(illiteracy)))
    checkParquetArray(cosh(illiteracy), cosh(as.array(illiteracy)))

    expect_error(cospi(illiteracy))

    checkParquetArray(sin(illiteracy), sin(as.array(illiteracy)))
    checkParquetArray(sinh(illiteracy), sinh(as.array(illiteracy)))

    expect_error(sinpi(illiteracy))

    checkParquetArray(tan(illiteracy), tan(as.array(illiteracy)))
    checkParquetArray(tanh(illiteracy), tanh(as.array(illiteracy)))

    expect_error(tanpi(illiteracy))

    checkParquetArray(gamma(illiteracy), gamma(as.array(illiteracy)))
    checkParquetArray(lgamma(illiteracy), lgamma(as.array(illiteracy)))

    expect_error(digamma(illiteracy))
    expect_error(trigamma(illiteracy))
})

test_that("Summary methods work as expected for a ParquetArray", {
    pqarray <- ParquetArray(titanic_path, key = dimnames(titanic_array), fact = "fate")
    expect_identical(max(pqarray), max(as.array(pqarray)))
    expect_identical(min(pqarray), min(as.array(pqarray)))
    expect_identical(range(pqarray), range(as.array(pqarray)))
    expect_equal(prod(pqarray), prod(as.array(pqarray)))
    expect_equal(sum(pqarray), sum(as.array(pqarray)))
    expect_identical(any(pqarray == 0L), any(as.array(pqarray) == 0L))
    expect_identical(all(pqarray == 0L), all(as.array(pqarray) == 0L))
})
