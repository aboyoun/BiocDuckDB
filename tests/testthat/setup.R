# Smoking, Alcohol and (O)esophageal Cancer
esoph_df <- esoph
for (i in 1:3) {
  esoph_df[[i]] <- as.character(esoph_df[[i]])
}
esoph_csv <- tempfile(fileext = ".csv")
write.csv(esoph_df, esoph_csv, row.names = FALSE)
esoph_csv_gz <- tempfile(fileext = ".csv.gz")
write.csv(esoph_df, gzfile(esoph_csv_gz), row.names = FALSE)
esoph_parquet <- tempfile(fileext = ".parquet")
arrow::write_parquet(esoph_df, esoph_parquet)


# Infertility after Spontaneous and Induced Abortion
infert_csv <- tempfile(fileext = ".csv")
write.csv(infert, infert_csv, row.names = FALSE)
infert_csv_gz <- tempfile(fileext = ".csv.gz")
write.csv(infert, gzfile(infert_csv_gz), row.names = FALSE)
infert_parquet <- tempfile(fileext = ".parquet")
arrow::write_parquet(infert, infert_parquet)

# Motor Trend Car Road Tests
mtcars_df <- cbind(model = rownames(mtcars), mtcars)
rownames(mtcars_df) <- NULL

mtcars_csv <- tempfile(fileext = ".csv")
write.csv(mtcars_df, mtcars_csv, row.names = FALSE)
mtcars_csv_gz <- tempfile(fileext = ".csv.gz")
write.csv(mtcars_df, gzfile(mtcars_csv_gz), row.names = FALSE)
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
titanic_csv <- tempfile(fileext = ".csv")
write.csv(titanic_df, titanic_csv, row.names = FALSE)
titanic_csv_gz <- tempfile(fileext = ".csv.gz")
write.csv(titanic_df, gzfile(titanic_csv_gz), row.names = FALSE)
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
sparse_csv <- tempfile(fileext = ".csv")
write.csv(sparse_df, sparse_csv, row.names = FALSE)
sparse_csv_gz <- tempfile(fileext = ".csv.gz")
write.csv(sparse_df, gzfile(sparse_csv_gz), row.names = FALSE)
sparse_parquet <- tempfile(fileext = ".parquet")
arrow::write_parquet(sparse_df, sparse_parquet)


# Special characters
special_df <- data.frame(id = letters[1:4], x = c(-Inf, 0, Inf, NaN))
special_path <- tempfile(fileext = ".parquet")
arrow::write_parquet(special_df, special_path)


# GRanges dataset
granges_df <- data.frame(id = head(letters, 10L),
                         seqnames = rep.int(c("chr2", "chr2", "chr1", "chr3"), c(1L, 3L, 2L, 4L)),
                         start = 1:10, end = 10L, width = 10:1,
                         strand = strand(rep.int(c("-", "+", "*", "+", "-"), c(1L, 2L, 2L, 3L, 2L))),
                         score = 1:10,
                         GC = seq(1, 0, length = 10),
                         group = rep(c("gr1", "gr2", "gr3", "gr4"), 1:4))
granges_tf <- tempfile(fileext = ".parquet")
arrow::write_parquet(granges_df, granges_tf)


# Helper functions
checkDuckDBTable <- function(object, expected) {
    expect_s4_class(object, "DuckDBTable")
    expect_gte(nrow(object), nrow(expected))
    expect_gte(NROW(object), NROW(expected))
    expect_equal(nkey(object) + ncol(object), ncol(expected))
    expect_equal(nkey(object) + NCOL(object), NCOL(expected))
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
    expect_identical(nrow(object), nrow(expected))
    expect_identical(ncol(object), ncol(expected))
    expect_setequal(rownames(object), rownames(expected))
    expect_identical(colnames(object), colnames(expected))
    if (nkey(object) == 0L) {
        object <- as.data.frame(object)
        expect_identical(nrow(object), nrow(expected))
        expect_identical(ncol(object), ncol(expected))
        expect_identical(colnames(object), colnames(expected))
    } else {
        expect_identical(as.data.frame(object)[rownames(expected), , drop=FALSE], expected)
    }
}

checkDuckDBColumn <- function(object, expected) {
    expect_s4_class(object, "DuckDBColumn")
    expect_identical(length(object), length(expected))
    if (nkey(object@table) == 0L) {
        object <- as.vector(object)
        expect_identical(length(object), length(expected))
    } else {
        expect_identical(names(object), names(expected))
        expect_equal(as.vector(object), expected)
    }
}


checkDuckDBGRanges <- function(object, expected) {
    expect_s4_class(object, "DuckDBGRanges")
    expect_identical(length(object), length(expected))
    if (nkey(object@frame) > 0L) {
        expect_setequal(names(object), names(expected))
        object <- object[names(expected)]
        expect_identical(unname(as.vector(seqnames(object))), as.character(seqnames(expected)))
        expect_identical(unname(as.vector(start(object))), start(expected))
        expect_identical(unname(as.vector(end(object))), end(expected))
        expect_identical(unname(as.vector(width(object))), width(expected))
        expect_identical(unname(as.vector(strand(object))), as.character(strand(expected)))
        df <- as.data.frame(expected)
        for (j in names(df)) {
            if (is.factor(df[[j]])) {
                df[[j]] <- as.character(df[[j]])
            }
        }
        expect_identical(as.data.frame(object)[names(expected), , drop=FALSE], df)
    }
}

checkDuckDBDataFrameList <- function(object, expected) {
    expect_s4_class(object, "DuckDBDataFrameList")
    expect_identical(length(object), length(expected))
    expect_identical(names(object), names(expected))
    expect_identical(NROW(object), NROW(expected))
    expect_identical(ROWNAMES(object), ROWNAMES(expected))
    expect_identical(elementNROWS(object), elementNROWS(expected))
    expect_identical(nrows(object), nrows(expected))
    expect_identical(ncols(object), ncols(expected))
    expect_identical(dims(object), dims(expected))
    for (i in seq_along(object)) {
        expect_setequal(rownames(object)[[i]], rownames(expected)[[i]])
    }
    expect_identical(colnames(object), colnames(expected))
    expect_identical(mcols(object), mcols(expected))
    expect_identical(columnMetadata(object), columnMetadata(expected))
    expect_identical(commonColnames(object), commonColnames(expected))
    checkDuckDBDataFrame(unlist(object), as.data.frame(unlist(expected, use.names = FALSE)))
}

checkDuckDBGRangesList <- function(object, expected) {
    expect_s4_class(object, "DuckDBGRangesList")
    expect_identical(length(object), length(expected))
    expect_identical(names(object), names(expected))
    expect_identical(elementNROWS(object), elementNROWS(expected))
    for (i in seq_along(object)) {
        expect_setequal(names(object[[i]]), names(expected[[i]]))
    }
    if (length(object) > 0L) {
        expect_identical(mcols(object), mcols(expected))
        checkDuckDBGRanges(unlist(object), unlist(expected, use.names = FALSE))
    }
}
