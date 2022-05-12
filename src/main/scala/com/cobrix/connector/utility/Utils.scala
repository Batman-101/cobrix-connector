package com.cobrix.connector.utility

import com.cobrix.connector.constants.ApplicationConstants
import java.sql.Timestamp

import com.typesafe.config.{Config, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import com.walmart.finance.cill.common.utilities.{HDFSUtility, PathUtility}
import com.walmart.finance.cill.feature.model.CillJobRun
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileAlreadyExistsException, FileSystem, FileUtil, Path}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import java.util.Date
import org.apache.spark.sql.functions.input_file_name


object Utils extends LazyLogging {

  /** This append the UUID in the given list of fields in the Configuration
   *
   * @param config           typesafe config
   * @param row              Row having data processing details
   * @return
   */
  def updateConfig(config: Config, row: Row): Config = {

    val UUID = Seq(config.getString("eventId"),
      row.getAs[String]("logical_batch_id")).mkString("/")

    val cillSourceWritePathColumn = config.getString("cillSourceWritePathColumn")
    val cillSourceReadPathColumn = config.getString("cillSourceReadPathColumn")

    val basePath = config.getString("basePath")
    val partitionKeyMap = row.getAs[String]("sink_partition_path").split("=")
    val partitionKeyVal = if (partitionKeyMap.size > 1) partitionKeyMap(1).toLong else 0
    val fileReadPath = PathUtility.combineStringPath(basePath, row.getAs[String](cillSourceReadPathColumn),
      row.getAs[String]("sink_partition_path"), row.getAs[String]("origin_feed_file")).toString

    val configWithStartTime = config.withValue("batchStartTime",
      ConfigValueFactory.fromAnyRef(Instant.now().atZone(ZoneId.of("UTC")).toInstant.toEpochMilli))
      .withValue("batchUUID", ConfigValueFactory.fromAnyRef(UUID))
      .withValue("baseWritePath", ConfigValueFactory.fromAnyRef(basePath + "/" + row.getAs[String](cillSourceWritePathColumn)))
      .withValue("errorPath", ConfigValueFactory.fromAnyRef(basePath + "/" +
        row.getAs[String](cillSourceReadPathColumn).replaceFirst("/in/", "/error/")))
      .withValue("reconPath", ConfigValueFactory.fromAnyRef(basePath + "/" +
        row.getAs[String](cillSourceWritePathColumn).replaceFirst("/in/", "/recon/")))
      .withValue("logicalBatchId", ConfigValueFactory.fromAnyRef(row.getAs[String]("logical_batch_id")))
      .withValue("baseReadPath", ConfigValueFactory.fromAnyRef(basePath + "/" +
        row.getAs[String](cillSourceReadPathColumn)))
      .withValue("partitionReadPath", ConfigValueFactory.fromAnyRef(basePath + "/" +
        row.getAs[String](cillSourceReadPathColumn) + "/" + row.getAs[String]("sink_partition_path")))
      .withValue("fileReadPath", ConfigValueFactory.fromAnyRef(fileReadPath))
      .withValue("fileErrorPath", ConfigValueFactory.fromAnyRef(fileReadPath.replaceFirst("/in/", "/error/")))
      .withValue("sourcePartitionPath", ConfigValueFactory.fromAnyRef(row.getAs[String]("sink_partition_path")))
      // only works if sinkPartitionPath = sourcePartitionPath
      .withValue("sinkPartitionPath", ConfigValueFactory.fromAnyRef(row.getAs[String]("sink_partition_path")))
      .withValue("partnKeyYrHr", ConfigValueFactory.fromAnyRef(partitionKeyVal))
      .withValue("sourceSystem", ConfigValueFactory.fromAnyRef(row.getAs[String]("source_system")))
      .withValue("originFeedFile", ConfigValueFactory.fromAnyRef(row.getAs[String]("origin_feed_file")))
      .withValue("eventId", ConfigValueFactory.fromAnyRef(java.util.UUID.randomUUID().toString))

    configWithStartTime
  }

  /** This append the UUID in the given list of fields in the Configuration
   *
   * @param config      typesafe config
   * @return CillJobRun
   */
  def createCillJobRun(config: Config): CillJobRun = {
    CillJobRun(
      config.getString("wfName"),
      config.getString("processName"),
      config.getString("sourceSystem"),
      config.getString("processingStage"),
      config.getString("baseReadPath"),
      config.getString("baseWritePath"),
      config.getString("errorMetaDataPath"),
      config.getLong("partnKeyYrHr")
    )
  }

  /**
   *
   * @param partitionKeyPath    Name of the Partition Column
   * @param partitionValuePath  Value of the Partition to be used
   * @param config              Config
   * @param path            BasePath (readPath | errorPath | writePath ..)
   * @return Given Path appended with the partition path
   */
  def getPartitionPath(partitionKeyPath: String, partitionValuePath: String)(config: Config, path: String): String = {
    val partnKeyYrHr = config.getString(partitionKeyPath)
    val partnKeyYrHrValue = config.getLong(partitionValuePath)
    s"${new Path(config.getString(path)).toString}/$partnKeyYrHr=$partnKeyYrHrValue"
  }

  /**
   * Takes Config and Path and returns Given Path appended with current partition path
   *
   * @param config Config
   * @param path   Path (readPath | errorPath | writePath ..)
   * @return
   */
  def getStagingPartitionPath(config: Config, path: String): String = {
    getPartitionPath("staging.partnKeyYrHr", "partnKeyYrHr")(config, path)
  }

  /** This method checks if files exists
   *
   * @param pathString Path String
   * @param hadoopConf Hadoop Configuration
   * @return
   */
  def doesPathExists(pathString: String, hadoopConf: Configuration): Boolean = {
    val fs = FileSystem.get(hadoopConf)
    val path = new Path(pathString)
    fs.exists(path) && fs.isDirectory(path)
  }

  /**
   * This method is used to configure different same mode types.
   * saveModeConfig can be Overwrite, Append or ErrorIfExists
   * Overwrite       - If writePath + partition exists deletes the directory and inserts data
   * Append          - Directly appends to write path
   * ErrorIfExists   - If writePath + partition exists throws error else proceeds and inserts data
   *
   * @param saveModeConfig Can either be Overwrite, Append or ErrorIfExists
   * @param hdConfig       Hadoop configurations
   * @param writePath      Final write path generated which should be concat of write path and partition key
   * @return SaveMode
   *
   * */
  def getPartitionSaveMode(saveModeConfig: String, hdConfig: Configuration, writePath: String): SaveMode =
    saveModeConfig.trim.toLowerCase match {
      case "overwrite" =>
        HDFSUtility.deleteHdfsPath(new Path(writePath), recursive = true, hdConfig)
        SaveMode.Append

      case "append" =>
        SaveMode.Append

      case "errorifexists" =>
        if (doesPathExists(writePath, hdConfig))
          throw new FileAlreadyExistsException(s"Write path - $writePath already exists")
        SaveMode.Append

      case _ => throw new IllegalArgumentException("Parameter saveMode passed is incorrect. Accepted values are Overwrite, Append or ErrorIfExists")
    }

  /** This method renames .bin.gz to .bin in GCS
   *
   * @param config typesafe Config
   * @param sparkSession SparkSession
   * @return Boolean
   */
  def renameGZFile(config: Config, sparkSession: SparkSession): Path = {

    val fileReadStrPath = config.getString("fileReadPath")
    val fileReadPath = new Path(fileReadStrPath)
    val fileTransferReadStrPath = PathUtility.combineStringPath(config.getString("basePath"),
      config.getString("checkpointDir"), config.getString("eventId"),
      fileReadPath.getName.replace(".gz", "")).toString
    val fileTransferReadPath = new Path(fileTransferReadStrPath)
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)

    if(fileReadStrPath.endsWith(".bin.gz") && fs.exists(new Path(fileReadStrPath)) &&
      FileUtil.copy(fs, fileReadPath, fs, fileTransferReadPath, false, sparkSession.sparkContext.hadoopConfiguration))
      fileTransferReadPath
    else
      fileReadPath
  }

  /** This method reads BIN file using cobrix connector
   *
   * @param spark SparkSession
   * @param config typesafe Config
   * @param readPath BIN input data path
   * @param copybookPath Copybook Path
   * @return DataFrame
   */
  def BINReader(spark: SparkSession, config: Config, readPath: String, copybookPath: String): DataFrame = {
    val truncateComments = config.getString("truncateComments")
    val schemaRetentionPolicy = config.getString("schemaRetentionPolicy")
    val encoding = config.getString("encoding")

    val readData = spark.read
      .format("cobol")
      .option("copybook", copybookPath)
      .option("truncate_comments", truncateComments)
      .option("schema_retention_policy", schemaRetentionPolicy)
      .option("encoding", encoding)
      .load(readPath)
      .withColumn(ApplicationConstants.INPUT_FILE_NAME, input_file_name())


    readData

  }

  /** This method fetches the file access time
   *
   * @param spark SparkSession
   * @param config typesafe Config
   * @return Time in String format
   */
  def getFileArrivalDateTime(spark: SparkSession, config: Config): String ={

    val filePath = config.getString("partitionReadPath")
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val status = fs.listStatus(new Path(filePath))

    convertTime(status.head.getModificationTime)

  }

  /** This method fetches the file access time
   *
   * @param time Time in Long
   * @return Time in HH:mm:ss format
   */
 def convertTime(time: Long): String = {
   val date = new Date(time)
   val format = new SimpleDateFormat("HH:mm:ss")
   format.format(date)
  }
  /*
  Delete checkout
   */
  def deleteCheckpoint(config: Config, spark: SparkSession): Boolean = {
    HDFSUtility.deleteHdfsPath(PathUtility.combineStringPath(config.getString("basePath"),
      config.getString("checkpointDir"), config.getString("eventId")), recursive = true,
      spark.sparkContext.hadoopConfiguration)
  }

  /**
   * This method is responsible for converting Integer date to Long Date Format
   *
   * @param date (String) :  Input Date in String Format
   * @return (TimestampType) : DateTime in TimeStampType
   */
  def dateTimeinMiliConvertor(date: String): Timestamp = {
    if (date != null) {
      val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH.mm.ss.SSS")
      java.sql.Timestamp.valueOf(java.time.LocalDateTime.parse(date, formatter))
    } else {
      null.asInstanceOf[Timestamp]
    }
  }

}
