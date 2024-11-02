setOldClass("tbl_duckdb_connection")

.getColumnType <- function(column) {
    if (inherits(column, "Date")) {
        "Date"
    } else {
        DelayedArray::type(column)
    }
}
