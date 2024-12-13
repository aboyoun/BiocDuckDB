#' Accessing DB Table Information
#'
#' Get a database table connection object
#'
#' @param x An object with a database table connection.
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
#' @name tblconn
NULL

#' @export
#' @rdname tblconn
setGeneric("tblconn", function(x) standardGeneric("tblconn"))
