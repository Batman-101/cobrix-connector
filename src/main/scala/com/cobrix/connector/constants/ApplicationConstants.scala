package com.cobrix.connector.constants

import com.cobrix.connector.rules.{AddColumn, DropColumn, ExtractNestedColumn, RenameColumn, UUIDColumn}

object ApplicationConstants {
  val UUID_FIELDS: List[String] = List("tmpErrorMetaDataPath")
  val BASE_WRITE_PATH = "baseWritePath"
  val ERROR_PATH = "errorPath"

  // TRANFORMER CONSTANTS
  val DROP =  "drop"
  val RENAME_COLUMN =  "renameColumn"
  val UUID_STRING =  "uuid"
  val EXTRACT_NESTED=  "extractNested"
  val ADD_COLUMN =  "addColumn"
  val DATE_TIME= "dateTime"
  val FILE_ARRIVAL_DATE_TIME = "fileArrivalDateTime"
  val INPUT_FILE_NAME = "input_file_name"
}
