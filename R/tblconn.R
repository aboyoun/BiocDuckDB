#' Accessing DB Table Information
#'
#' Get a database table connection object
#'
#' @param x An object with a database table connection.
#' @param select A logical value indicating whether to apply column
#'        selections to the database table connection.
#' @param filter A logical value indicating whether to apply key column
#'        filtering to the database table connection.
#'
#' @author Patrick Aboyoun
#'
#' @return
#' returns a database table connection object.
#'
#' @examples
#' tblconn
#' showMethods("tableconn")
#'
#' @aliases
#' tblconn
#'
#' @keywords IO
#'
#' @name tblconn
NULL

#' @export
#' @rdname tblconn
setGeneric("tblconn", function(x, select = TRUE, filter = TRUE) standardGeneric("tblconn"))
