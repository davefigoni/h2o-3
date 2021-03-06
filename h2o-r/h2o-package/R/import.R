##`'
##`' Data Import
##`'
##`' Importing data is a _lazy_ parse of the data. It adds an extra step so that a user may specify a variety of options
##`' including a header file, separator type, and in the future column type. Additionally, the import phase provides
##`' feedback on whether or not a folder or group of files may be imported together.

#'
#' Import Files into H2O
#'
#' Imports files into an H2O cloud. The default behavior is to pass-through to the parse phase
#' automatically.
#'
#' Other than \code{h2o.uploadFile}, if the given path is relative, then it will be relative to the
#' start location of the H2O instance. Additionally, the file must be on the same machine as the H2O
#' cloud. In the case of \code{h2o.uploadFile}, a relative path will resolve relative to the working
#' directory of the current R session.
#'
#' Import an entire directory of files. If the given path is relative, then it
#' will be relative to the start location of the H2O instance. The default
#' behavior is to pass-through to the parse phase automatically.
#'
#' \code{h2o.importURL} and \code{h2o.importHDFS} are both deprecated functions. Instead, use
#' \code{h2o.importFile}
#'
#' @param path The complete URL or normalized file path of the file to be
#'        imported. Each row of data appears as one line of the file.
#' @param pattern (Optional) Character string containing a regular expression to match file(s) in
#'        the folder.
#' @param destination_frame (Optional) The unique hex key assigned to the imported file. If
#'        none is given, a key will automatically be generated based on the URL
#'        path.
#' @param parse (Optional) A logical value indicating whether the file should be
#'        parsed after import.
#' @param header (Optional) A logical value indicating whether the first line of
#'        the file contains column headers. If left empty, the parser will try
#'        to automatically detect this.
#' @param sep (Optional) The field separator character. Values on each line of
#'        the file are separated by this character. If \code{sep = ""}, the
#'        parser will automatically detect the separator.
#' @param col.names (Optional) An H2OFrame object containing a single
#'        delimited line with the column names for the file.
#' @param col.types (Optional) A vector to specify whether columns should be
#'        forced to a certain type upon import parsing.
#' @param na.strings (Optional) H2O will interpret these strings as missing.
#' @param parse_type (Optional) Specify which parser type H2O will use.
#'        Valid types are "ARFF", "XLS", "CSV", "SVMLight"
#' @param progressBar (Optional) When FALSE, tell H2O parse call to block
#'        synchronously instead of polling.  This can be faster for small
#'        datasets but loses the progress bar.
#' @examples
#' \donttest{
#' h2o.init(ip = "localhost", port = 54321, startH2O = TRUE)
#' prosPath = system.file("extdata", "prostate.csv", package = "h2o")
#' prostate.hex = h2o.uploadFile(path = prosPath, destination_frame = "prostate.hex")
#' class(prostate.hex)
#' summary(prostate.hex)
#' }
#' @name h2o.importFile
#' @export
h2o.importFolder <- function(path, pattern = "", destination_frame = "", parse = TRUE, header = NA, sep = "",
                             col.names = NULL, col.types=NULL, na.strings=NULL) {
  if(!is.character(path) || is.na(path) || !nzchar(path)) stop("`path` must be a non-empty character string")
  if(!is.character(pattern) || length(pattern) != 1L || is.na(pattern)) stop("`pattern` must be a character string")
  .key.validate(destination_frame)
  if(!is.logical(parse) || length(parse) != 1L || is.na(parse))
    stop("`parse` must be TRUE or FALSE")

  if(length(path) > 1L) {
    destFrames <- c()
    fails <- c()
    for(path2 in path){
      res <-.h2o.__remoteSend(.h2o.__IMPORT, path=path2)
      destFrames <- c(destFrames, res$destination_frames)
      fails <- c(fails, res$fails)
    }
    res$destination_frames <- destFrames
    res$fails <- fails
  } else {
    res <- .h2o.__remoteSend(.h2o.__IMPORT, path=path)
  }

  if(length(res$fails) > 0L) {
    for(i in seq_len(length(res$fails)))
      cat(res$fails[[i]], "failed to import")
  }
  # Return only the files that successfully imported
  if(length(res$files) <= 0L) stop("all files failed to import")
  if(parse) {
    srcKey <- res$destination_frames
    return( h2o.parseRaw(data=.newH2OFrame(op="ImportFolder",id=srcKey,-1,-1), destination_frame=destination_frame,
                         header=header, sep=sep, col.names=col.names, col.types=col.types, na.strings=na.strings) )
  }
  myData <- lapply(res$destination_frames, function(x) .newH2OFrame( op="ImportFolder", id=x,-1,-1))  # do not gc, H2O handles these nfs:// vecs
  if(length(res$destination_frames) == 1L)
    return( myData[[1L]] )
  else
    return( myData )
}


#' @export
h2o.importFile <- function(path, destination_frame = "", parse = TRUE, header=NA, sep = "", col.names=NULL,
                           col.types=NULL, na.strings=NULL) {
  h2o.importFolder(path, pattern = "", destination_frame=destination_frame, parse, header, sep, col.names, col.types,
                   na.strings=na.strings)
}


#' @rdname h2o.importFile
#' @export
h2o.importURL <- function(path, destination_frame = "", parse = TRUE, header = NA, sep = "", col.names = NULL, na.strings=NULL) {
  .Deprecated("h2o.importFile")
}


#' @rdname h2o.importFile
#' @export
h2o.importHDFS <- function(path, pattern = "", destination_frame = "", parse = TRUE, header = NA, sep = "", col.names = NULL, na.strings=NULL) {
  .Deprecated("h2o.importFolder")
}


#' @rdname h2o.importFile
#' @export
h2o.uploadFile <- function(path, destination_frame = "",
                           parse = TRUE, header = NA, sep = "", col.names = NULL,
                           col.types = NULL, na.strings = NULL, progressBar = FALSE, parse_type=NULL) {
  if(!is.character(path) || length(path) != 1L || is.na(path) || !nzchar(path))
    stop("`path` must be a non-empty character string")
  .key.validate(destination_frame)
  if(!is.logical(parse) || length(parse) != 1L || is.na(parse))
    stop("`parse` must be TRUE or FALSE")
  if(!is.logical(progressBar) || length(progressBar) != 1L || is.na(progressBar))
    stop("`progressBar` must be TRUE or FALSE")

  .h2o.gc()  # Clear out H2O to make space for new file
  path <- normalizePath(path, winslash = "/")
  srcKey <- .key.make( path )
  urlSuffix <- sprintf("PostFile?destination_frame=%s",  curlEscape(srcKey))
  fileUploadInfo <- fileUpload(path)
  .h2o.doSafePOST(h2oRestApiVersion = .h2o.__REST_API_VERSION, urlSuffix = urlSuffix, fileUploadInfo = fileUploadInfo)

  rawData <- .newH2OFrame(op="PostFile",id=srcKey,-1,-1)
  destination_frame <- if( destination_frame == "" ) .key.make(strsplit(basename(path), "\\.")[[1]][1]) else destination_frame
  if (parse) {
    h2o.parseRaw(data=rawData, destination_frame=destination_frame, header=header, sep=sep, col.names=col.names, col.types=col.types, na.strings=na.strings, blocking=!progressBar, parse_type = parse_type)
  } else {
    rawData
  }
}

#'
#' Import SQL Table into H2O
#'
#' Imports SQL table into an H2O cloud. Assumes that the SQL table is not being updated and is stable.
#' Runs multiple SELECT SQL queries concurrently for parallel ingestion.
#' Be sure to start the h2o.jar in the terminal with your downloaded JDBC driver in the classpath:
#'    `java -cp <path_to_h2o_jar>:<path_to_jdbc_driver_jar> water.H2OApp`
#' Also see h2o.import_sql_select.
#' Currently supported SQL databases are MySQL, PostgreSQL, and MariaDB. Support for Oracle 12g and Microsoft SQL Server 
#  is forthcoming.
#'
#' For example, 
#'    my_sql_conn_url <- "jdbc:mysql://172.16.2.178:3306/ingestSQL?&useSSL=false"
#'    table <- "citibike20k"
#'    username <- "root"
#'    password <- "abc123"
#'    my_citibike_data <- h2o.import_sql_table(my_sql_conn_url, table, username, password)
#'
#' @param connection_url URL of the SQL database connection as specified by the Java Database Connectivity (JDBC) Driver.
#'        For example, "jdbc:mysql://localhost:3306/menagerie?&useSSL=false"
#' @param table Name of SQL table
#' @param username Username for SQL server
#' @param password Password for SQL server
#' @param columns (Optional) Character vector of column names to import from SQL table. Default is to import all columns. 
#' @param optimize (Optional) Optimize import of SQL table for faster imports. Experimental. Default is true. 
#' @export
h2o.import_sql_table <- function(connection_url, table, username, password, columns = NULL, optimize = NULL) {
  parms <- list()
  parms$connection_url <- connection_url
  parms$table <- table
  parms$username <- username
  parms$password <- password
  if (!is.null(columns)) {
    columns <- toString(columns)
    parms$columns <- columns
  }
  if (!is.null(optimize)) parms$optimize <- optimize
  res <- .h2o.__remoteSend('ImportSQLTable', method = "POST", .params = parms, h2oRestApiVersion = 99)
  job_key <- res$key$name
  dest_key <- res$dest$name
  .h2o.__waitOnJob(job_key)
  h2o.getFrame(dest_key)
}

#'
#' Import SQL table that is result of SELECT SQL query into H2O
#'
#' Creates a temporary SQL table from the specified sql_query.
#' Runs multiple SELECT SQL queries on the temporary table concurrently for parallel ingestion, then drops the table.
#' Be sure to start the h2o.jar in the terminal with your downloaded JDBC driver in the classpath:
#'    `java -cp <path_to_h2o_jar>:<path_to_jdbc_driver_jar> water.H2OApp`
#' Also see h2o.import_sql_table.
#' Currently supported SQL databases are MySQL, PostgreSQL, and MariaDB. Support for Oracle 12g and Microsoft SQL Server 
#  is forthcoming.   
#'
#' For example, 
#'    my_sql_conn_url <- "jdbc:mysql://172.16.2.178:3306/ingestSQL?&useSSL=false"
#'    select_query <- "SELECT bikeid from citibike20k"
#'    username <- "root"
#'    password <- "abc123"
#'    my_citibike_data <- h2o.import_sql_select(my_sql_conn_url, select_query, username, password)
#'
#' @param connection_url URL of the SQL database connection as specified by the Java Database Connectivity (JDBC) Driver.
#'        For example, "jdbc:mysql://localhost:3306/menagerie?&useSSL=false"
#' @param select_query SQL query starting with `SELECT` that returns rows from one or more database tables.
#' @param username Username for SQL server
#' @param password Password for SQL server
#' @param optimize (Optional) Optimize import of SQL table for faster imports. Experimental. Default is true. 
#' @export
h2o.import_sql_select<- function(connection_url, select_query, username, password, optimize = NULL) {
  parms <- list()
  parms$connection_url <- connection_url
  parms$select_query <- select_query
  parms$username <- username
  parms$password <- password
  if (!is.null(optimize)) parms$optimize <- optimize
  res <- .h2o.__remoteSend('ImportSQLTable', method = "POST", .params = parms, h2oRestApiVersion = 99)
  job_key <- res$key$name
  dest_key <- res$dest$name
  .h2o.__waitOnJob(job_key)
  h2o.getFrame(dest_key)
}


#'
#' Load H2O Model from HDFS or Local Disk
#'
#' Load a saved H2O model from disk.
#'
#' @param path The path of the H2O Model to be imported.
#'        and port of the server running H2O.
#' @return Returns a \linkS4class{H2OModel} object of the class corresponding to the type of model
#'         built.
#' @seealso \code{\link{h2o.saveModel}, \linkS4class{H2OModel}}
#' @examples
#' \dontrun{
#' # library(h2o)
#' # h2o.init()
#' # prosPath = system.file("extdata", "prostate.csv", package = "h2o")
#' # prostate.hex = h2o.importFile(path = prosPath, destination_frame = "prostate.hex")
#' # prostate.glm = h2o.glm(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"),
#' #   training_frame = prostate.hex, family = "binomial", alpha = 0.5)
#' # glmmodel.path = h2o.saveModel(prostate.glm, dir = "/Users/UserName/Desktop")
#' # glmmodel.load = h2o.loadModel(glmmodel.path)
#' }
#' @export
h2o.loadModel <- function(path) {
  if(!is.character(path) || length(path) != 1L || is.na(path) || !nzchar(path))
    stop("`path` must be a non-empty character string")

  res <- .h2o.__remoteSend(.h2o.__LOAD_MODEL, h2oRestApiVersion = 99, dir = path, method = "POST")$models[[1L]]
  res
  h2o.getModel(res$model_id$name)
}
