# Tests the basic functions of a ParquetArraySeed.
# library(testthat); library(ParquetDataFrame); source("setup.R"); source("test-ParquetArraySeed.R")

test_that("basic methods work as expected for a ParquetArraySeed", {
    seed <- ParquetArraySeed(titanic_path, key = dimnames(titanic_array), fact = "fate")
    checkParquetArraySeed(seed, titanic_array)

    seed <- ParquetArraySeed(titanic_path, key = dimnames(titanic_array), fact = "fate", type = "double")
    expect_s4_class(seed, "ParquetArraySeed")
    expect_identical(type(seed), "double")
    expect_identical(length(seed), length(titanic_array))
    expect_identical(dim(seed), dim(titanic_array))
    expect_identical(dimnames(seed), dimnames(titanic_array))
    expect_equal(as.array(seed), titanic_array)

    seed <- ParquetArraySeed(titanic_path, key = dimnames(titanic_array), fact = "fate", type = "character")
    expect_s4_class(seed, "ParquetArraySeed")
    expect_identical(type(seed), "character")
    expect_identical(length(seed), length(titanic_array))
    expect_identical(dim(seed), dim(titanic_array))
    expect_identical(dimnames(seed), dimnames(titanic_array))
})

test_that("extraction methods work as expected for a ParquetArraySeed", {
    seed <- ParquetArraySeed(titanic_path, key = dimnames(titanic_array), fact = "fate")

    expect_error(seed[,])

    object <- seed[]
    checkParquetArraySeed(object, titanic_array)

    object <- seed[, 2:1, , ]
    expected <- titanic_array[, 2:1, , ]
    checkParquetArraySeed(object, expected)

    object <- seed[c(4, 2), , 1, ]
    expected <- titanic_array[c(4, 2), , 1, ]
    checkParquetArraySeed(object, expected)

    object <- seed[c(4, 2), , 1, , drop = FALSE]
    expected <- titanic_array[c(4, 2), , 1, , drop = FALSE]
    checkParquetArraySeed(object, expected)

    object <- seed[4, 2, 1, 2]
    expected <- as.array(titanic_array[4, 2, 1, 2])
    checkParquetArraySeed(object, expected)

    object <- seed[4, 2, 1, 2, drop = FALSE]
    expected <- titanic_array[4, 2, 1, 2, drop = FALSE]
    checkParquetArraySeed(object, expected)

    object <- seed[c("1st", "2nd", "3rd"), "Female", "Child", ]
    expected <- titanic_array[c("1st", "2nd", "3rd"), "Female", "Child", ]
    checkParquetArraySeed(object, expected)
})

test_that("aperm and t methods work as expected for a ParquetArraySeed", {
    seed <- ParquetArraySeed(titanic_path, key = dimnames(titanic_array), fact = "fate")

    object <- aperm(seed, c(4, 2, 1, 3))
    expected <- aperm(titanic_array, c(4, 2, 1, 3))
    checkParquetArraySeed(object, expected)

    names(dimnames(state.x77)) <- c("rowname", "colname")
    seed <- ParquetArraySeed(state_path, key = dimnames(state.x77), fact = "value")

    object <- t(seed)
    expected <- t(state.x77)
    expect_s4_class(object, "ParquetArraySeed")
    expect_identical(type(object), "double")
    expect_identical(length(object), length(expected))
    expect_identical(dim(object), dim(expected))
    expect_identical(dimnames(object)[[1L]], dimnames(expected)[[1L]])
    expect_setequal(dimnames(object)[[2L]], dimnames(expected)[[2L]])
    expect_identical(as.array(object)[, colnames(expected)], expected)
})

test_that("Arith methods work as expected for a ParquetArraySeed", {
    seed <- ParquetArraySeed(titanic_path, key = dimnames(titanic_array), fact = "fate")

    ## "+"
    checkParquetArraySeed(seed + sqrt(seed), as.array(seed) + sqrt(as.array(seed)))
    checkParquetArraySeed(seed + 1L, as.array(seed) + 1L)
    checkParquetArraySeed(seed + 3.14, as.array(seed) + 3.14)
    checkParquetArraySeed(1L + seed, 1L + as.array(seed))
    checkParquetArraySeed(3.14 + seed, 3.14 + as.array(seed))

    ## "-"
    checkParquetArraySeed(seed - sqrt(seed), as.array(seed) - sqrt(as.array(seed)))
    checkParquetArraySeed(seed - 1L, as.array(seed) - 1L)
    checkParquetArraySeed(seed - 3.14, as.array(seed) - 3.14)
    checkParquetArraySeed(1L - seed, 1L - as.array(seed))
    checkParquetArraySeed(3.14 - seed, 3.14 - as.array(seed))

    ## "*"
    checkParquetArraySeed(seed * sqrt(seed), as.array(seed) * sqrt(as.array(seed)))
    checkParquetArraySeed(seed * 1L, as.array(seed) * 1L)
    checkParquetArraySeed(seed * 3.14, as.array(seed) * 3.14)
    checkParquetArraySeed(1L * seed, 1L * as.array(seed))
    checkParquetArraySeed(3.14 * seed, 3.14 * as.array(seed))

    ## "/"
    checkParquetArraySeed(seed / sqrt(seed), as.array(seed) / sqrt(as.array(seed)))
    checkParquetArraySeed(seed / 1L, as.array(seed) / 1L)
    checkParquetArraySeed(seed / 3.14, as.array(seed) / 3.14)
    checkParquetArraySeed(1L / seed, 1L / as.array(seed))
    checkParquetArraySeed(3.14 / seed, 3.14 / as.array(seed))

    ## "^"
    checkParquetArraySeed(seed ^ sqrt(seed), as.array(seed) ^ sqrt(as.array(seed)))
    checkParquetArraySeed(seed ^ 3.14, as.array(seed) ^ 3.14)
    checkParquetArraySeed(3.14 ^ seed, 3.14 ^ as.array(seed))

    ## "%%"
    checkParquetArraySeed(seed %% sqrt(seed), as.array(seed) %% sqrt(as.array(seed)))
    checkParquetArraySeed(seed %% 3.14, as.array(seed) %% 3.14)
    checkParquetArraySeed(3.14 %% seed, 3.14 %% as.array(seed))

    ## "%/%"
    checkParquetArraySeed(seed %/% sqrt(seed), as.array(seed) %/% sqrt(as.array(seed)))
    checkParquetArraySeed(seed %/% 3.14, as.array(seed) %/% 3.14)
    checkParquetArraySeed(3.14 %/% seed, 3.14 %/% as.array(seed))
})

test_that("Compare methods work as expected for a ParquetArraySeed", {
    seed <- ParquetArraySeed(titanic_path, key = dimnames(titanic_array), fact = "fate")

    ## "=="
    checkParquetArraySeed(seed == sqrt(seed), as.array(seed) == sqrt(as.array(seed)))
    checkParquetArraySeed(seed == 1L, as.array(seed) == 1L)
    checkParquetArraySeed(seed == 3.14, as.array(seed) == 3.14)
    checkParquetArraySeed(1L == seed, 1L == as.array(seed))
    checkParquetArraySeed(3.14 == seed, 3.14 == as.array(seed))

    ## ">"
    checkParquetArraySeed(seed > sqrt(seed), as.array(seed) > sqrt(as.array(seed)))
    checkParquetArraySeed(seed > 1L, as.array(seed) > 1L)
    checkParquetArraySeed(seed > 3.14, as.array(seed) > 3.14)
    checkParquetArraySeed(1L > seed, 1L > as.array(seed))
    checkParquetArraySeed(3.14 > seed, 3.14 > as.array(seed))

    ## "<"
    checkParquetArraySeed(seed < sqrt(seed), as.array(seed) < sqrt(as.array(seed)))
    checkParquetArraySeed(seed < 1L, as.array(seed) < 1L)
    checkParquetArraySeed(seed < 3.14, as.array(seed) < 3.14)
    checkParquetArraySeed(1L < seed, 1L < as.array(seed))
    checkParquetArraySeed(3.14 < seed, 3.14 < as.array(seed))

    ## "!="
    checkParquetArraySeed(seed != sqrt(seed), as.array(seed) != sqrt(as.array(seed)))
    checkParquetArraySeed(seed != 1L, as.array(seed) != 1L)
    checkParquetArraySeed(seed != 3.14, as.array(seed) != 3.14)
    checkParquetArraySeed(1L != seed, 1L != as.array(seed))
    checkParquetArraySeed(3.14 != seed, 3.14 != as.array(seed))

    ## "<="
    checkParquetArraySeed(seed <= sqrt(seed), as.array(seed) <= sqrt(as.array(seed)))
    checkParquetArraySeed(seed <= 1L, as.array(seed) <= 1L)
    checkParquetArraySeed(seed <= 3.14, as.array(seed) <= 3.14)
    checkParquetArraySeed(1L <= seed, 1L <= as.array(seed))
    checkParquetArraySeed(3.14 <= seed, 3.14 <= as.array(seed))

    ## ">="
    checkParquetArraySeed(seed >= sqrt(seed), as.array(seed) >= sqrt(as.array(seed)))
    checkParquetArraySeed(seed >= 1L, as.array(seed) >= 1L)
    checkParquetArraySeed(seed >= 3.14, as.array(seed) >= 3.14)
    checkParquetArraySeed(1L >= seed, 1L >= as.array(seed))
    checkParquetArraySeed(3.14 >= seed, 3.14 >= as.array(seed))
})

test_that("Logic methods work as expected for a ParquetArraySeed", {
    seed <- ParquetArraySeed(titanic_path, key = dimnames(titanic_array), fact = "fate")

    ## "&"
    x <- seed > 70
    y <- seed < 4000
    checkParquetArraySeed(x & y, as.array(x) & as.array(y))

    ## "|"
    x <- seed > 70
    y <- sqrt(seed) > 0
    checkParquetArraySeed(x | y, as.array(x) | as.array(y))
})

test_that("Math methods work as expected for a ParquetArraySeed", {
    names(dimnames(state.x77)) <- c("rowname", "colname")
    seed <- ParquetArraySeed(state_path, key = dimnames(state.x77), fact = "value")

    income <- seed[, "Income"]
    ikeep <-
      c("Colorado", "Delaware", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
        "Maine", "Maryland", "Michigan", "Minnesota", "Missouri", "Montana",
        "Nebraska", "Nevada", "New Hampshire", "North Dakota", "Ohio", "Oregon",
        "Pennsylvania", "South Dakota", "Utah", "Vermont", "Washington", "Wisconsin",
        "Wyoming")
    illiteracy <- seed[ikeep, "Illiteracy"]

    checkParquetArraySeed(abs(income), abs(as.array(income)))
    checkParquetArraySeed(sqrt(income), sqrt(as.array(income)))
    checkParquetArraySeed(ceiling(income), ceiling(as.array(income)))
    checkParquetArraySeed(floor(income), floor(as.array(income)))
    checkParquetArraySeed(trunc(income), trunc(as.array(income)))

    expect_error(cummax(income))
    expect_error(cummin(income))
    expect_error(cumprod(income))
    expect_error(cumsum(income))

    checkParquetArraySeed(log(income), log(as.array(income)))
    checkParquetArraySeed(log10(income), log10(as.array(income)))
    checkParquetArraySeed(log2(income), log2(as.array(income)))

    expect_error(log1p(income))

    checkParquetArraySeed(acos(illiteracy), acos(as.array(illiteracy)))
    checkParquetArraySeed(acosh(income), acosh(as.array(income)))
    checkParquetArraySeed(asin(illiteracy), asin(as.array(illiteracy)))
    checkParquetArraySeed(asinh(income), asinh(as.array(income)))
    checkParquetArraySeed(atan(income), atan(as.array(income)))
    checkParquetArraySeed(atanh(illiteracy), atanh(as.array(illiteracy)))

    checkParquetArraySeed(exp(income), exp(as.array(income)))

    expect_error(expm1(income))

    checkParquetArraySeed(cos(illiteracy), cos(as.array(illiteracy)))
    checkParquetArraySeed(cosh(illiteracy), cosh(as.array(illiteracy)))

    expect_error(cospi(illiteracy))

    checkParquetArraySeed(sin(illiteracy), sin(as.array(illiteracy)))
    checkParquetArraySeed(sinh(illiteracy), sinh(as.array(illiteracy)))

    expect_error(sinpi(illiteracy))

    checkParquetArraySeed(tan(illiteracy), tan(as.array(illiteracy)))
    checkParquetArraySeed(tanh(illiteracy), tanh(as.array(illiteracy)))

    expect_error(tanpi(illiteracy))

    checkParquetArraySeed(gamma(illiteracy), gamma(as.array(illiteracy)))
    checkParquetArraySeed(lgamma(illiteracy), lgamma(as.array(illiteracy)))

    expect_error(digamma(illiteracy))
    expect_error(trigamma(illiteracy))
})
