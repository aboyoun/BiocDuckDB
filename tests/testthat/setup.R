# Smoking, Alcohol and (O)esophageal Cancer
esoph_df <- esoph
for (i in 1:3) {
  esoph_df[[i]] <- as.character(esoph_df[[i]])
}
esoph_parquet <- tempfile(fileext = ".parquet")
arrow::write_parquet(esoph_df, esoph_parquet)

# Infertility after Spontaneous and Induced Abortion
infert_parquet <- tempfile(fileext = ".parquet")
arrow::write_parquet(infert, infert_parquet)

# Motor Trend Car Road Tests
mtcars_df <- cbind(model = rownames(mtcars), mtcars)
rownames(mtcars_df) <- NULL
mtcars_parquet <- tempfile(fileext = ".parquet")
arrow::write_parquet(mtcars_df, mtcars_parquet)
mtcars_mcols <- DataFrame(description = c("Miles/(US) gallon", "Number of cylinders",
                                          "Displacement (cu.in.)", "Gross horsepower",
                                          "Rear axle ratio", "Weight (1000 lbs)",
                                          "1/4 mile time", "Engine (0 = V-shaped, 1 = straight)",
                                          "Transmission (0 = automatic, 1 = manual)",
                                          "Number of forward gears", "Number of carburetors"),
                          row.names = colnames(mtcars))

# State dataset
state_df <- data.frame(
  region = rep(as.character(state.region), times = ncol(state.x77)),
  division = rep(as.character(state.division), times = ncol(state.x77)),
  rowname = rep(rownames(state.x77), times = ncol(state.x77)),
  colname = rep(colnames(state.x77), each = nrow(state.x77)),
  value = as.vector(state.x77)
)
state_path <- tempfile()
arrow::write_dataset(state_df, state_path, format = "parquet", partitioning = c("region", "division"))

# Titanic dataset
titanic_array <- unclass(Titanic)
storage.mode(titanic_array) <- "integer"
titanic_df <- do.call(expand.grid, c(dimnames(Titanic), stringsAsFactors = FALSE))
titanic_df$fate <- as.integer(Titanic[as.matrix(titanic_df)])
titanic_df[titanic_df$fate != 0L, ]
titanic_parquet <- tempfile(fileext = ".parquet")
arrow::write_parquet(titanic_df, titanic_parquet)

# Random array
set.seed(123)
sparse_df <- data.frame(dim1 = sample(LETTERS, 1000, replace = TRUE),
                        dim2 = sample(letters, 1000, replace = TRUE),
                        dim3 = sample(month.abb, 1000, replace = TRUE),
                        value = sample(100L, 1000, replace = TRUE))
sparse_df <- sparse_df[!duplicated(sparse_df[,1:3]), ]
sparse_df <- sparse_df[order(sparse_df$dim1, sparse_df$dim2, sparse_df$dim3),]
rownames(sparse_df) <- NULL
sparse_array <- array(0L, dim = c(26L, 26L, 12L), dimnames = list(dim1 = LETTERS, dim2 = letters, dim3 = month.abb))
sparse_array[as.matrix(sparse_df[,1:3])] <- sparse_df[["value"]]
sparse_parquet <- tempfile(fileext = ".parquet")
arrow::write_parquet(sparse_df, sparse_parquet)


# Helper functions
checkDuckDBTable <- function(object, expected) {
    expect_s4_class(object, "DuckDBTable")
    expect_gte(nrow(object), nrow(expected))
    expect_equal(nkey(object) + ncol(object), ncol(expected))
    expect_identical(c(keynames(object), colnames(object)), colnames(expected))
    if (nkey(object) == 0L) {
        object <- as.data.frame(object)
        expect_gte(nrow(object), nrow(expected))
        expect_equal(ncol(object) - 1L, ncol(expected))
    } else {
        df <- as.data.frame(object)
        df <- df[match(do.call(paste, expected[, keynames(object), drop = FALSE]),
                       do.call(paste, df[, keynames(object), drop = FALSE])), ]
        rownames(df) <- NULL
        expect_equivalent(df, expected)
    }
}

checkDuckDBArraySeed <- function(object, expected) {
    expect_s4_class(object, "DuckDBArraySeed")
    expect_identical(type(object), type(expected))
    expect_identical(length(object), length(expected))
    expect_identical(dim(object), dim(expected))
    expect_identical(dimnames(object), dimnames(expected))
    expect_equal(as.array(object), expected)
}

checkDuckDBArray <- function(object, expected) {
    expect_s4_class(object, "DuckDBArray")
    expect_identical(type(object), type(expected))
    expect_identical(length(object), length(expected))
    expect_identical(dim(object), dim(expected))
    expect_identical(dimnames(object), dimnames(expected))
    expect_equal(as.array(object), expected)
}

checkDuckDBMatrix <- function(object, expected) {
    expect_s4_class(object, "DuckDBMatrix")
    expect_identical(type(object), typeof(expected))
    expect_identical(length(object), length(expected))
    expect_identical(dim(object), dim(expected))
    expect_identical(dimnames(object), dimnames(expected))
    expect_equal(as.matrix(object), expected)
}

checkDuckDBDataFrame <- function(object, expected) {
    expect_s4_class(object, "DuckDBDataFrame")
    expect_identical(ncol(object), ncol(expected))
    expect_identical(nrow(object), nrow(expected))
    expect_setequal(rownames(object), rownames(expected))
    expect_identical(colnames(object), colnames(expected))
    if (nkey(object) == 0L) {
        object <- as.data.frame(object)
        expect_identical(ncol(object), ncol(expected))
        expect_identical(nrow(object), nrow(expected))
        expect_identical(colnames(object), colnames(expected))
    } else {
        expect_identical(as.data.frame(object)[rownames(expected), , drop=FALSE], expected)
    }
}

checkDuckDBColumn <- function(object, expected) {
    expect_s4_class(object, "DuckDBColumn")
    expect_identical(length(object), length(expected))
    expect_identical(names(object), names(expected))
    expect_equal(as.vector(object), expected)
}
