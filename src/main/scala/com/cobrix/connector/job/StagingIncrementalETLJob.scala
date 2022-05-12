package com.cobrix.connector.job

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import com.walmart.finance.cill.common.writer.ORCWriter
import com.walmart.finance.cill.feature.job.CillIncrementalETLJob
import com.walmart.finance.cill.feature.model.{CillErrorLog, CillJobRun}
import com.walmart.finance.cill.feature.utility.errorlog.ErrorLogGenerator
import com.walmart.finance.cill.feature.utility.errorlog.ErrorLogWriter.persistErrorLog
import com.walmart.finance.cill.feature.utility.errorlog.UtilityFunctions.createCillErrorLog
import com.walmart.finance.cill.feature.utility.incremental.StatelessUtils
import com.walmart.finance.cill.feature.utility.joblog.AddLineDetail
import com.cobrix.connector.constants.ApplicationConstants._
import com.cobrix.connector.rules.{AddColumn, DateTime, DropColumn, ExtractNestedColumn, FileArrivalDateTime, RenameColumn, UUIDColumn}
import com.walmart.finance.cill.financialtransaction.rules._
import com.cobrix.connector.utility.PipelineUtilities.configToPipeline
import com.cobrix.connector.utility.Utils._
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}



object StagingIncrementalETLJob extends CillIncrementalETLJob with LazyLogging {
  override def getIncrementalWorkLog(config: Config, sparkSession: SparkSession): DataFrame = {
    logger.info("Started getting incremental worklog")

    val incrementalDf = StatelessUtils.getIncrementalWorkLog(config, sparkSession)
    logger.info("Successfully fetched incremental worklog")

    incrementalDf
  }

  override def updateBatchConfig(config: Config, sparkSession: SparkSession, row: Row): Config = {
    logger.info("Started updating configuration  for logicalBatchId : {}", row.getAs[String]("logical_batch_id"))

    val updatedConfig = updateConfig(config, row)
    logger.info("Completed updating configuration  for logicalBatchId : {}", updatedConfig.getString("logicalBatchId"))

    updatedConfig
  }


  override def readBatchData(config: Config, sparkSession: SparkSession): DataFrame = {
    logger.info("Started reading data from partition {}", config.getString("sourcePartitionPath") +
      "/" + config.getString("originFeedFile"))

    val copybookPath = config.getString("copybookPath")

    val readPath = renameGZFile(config, sparkSession).toString

    val readData = BINReader(sparkSession,config, readPath, copybookPath)

    logger.info("Completed reading data from partition {}", config.getString("sourcePartitionPath"))

    readData
  }

  override def transformBatchData(config: Config, sparkSession: SparkSession, dataFrame: DataFrame): DataFrame = {
    logger.info("Started batch transformation  for logicalBatchId : {}",
      config.getString("logicalBatchId"))

    val cillJobRunDetails: CillJobRun = createCillJobRun(config)

    implicit val configVal: Config = config
    implicit val spark: SparkSession = sparkSession
    val nweRules = configToPipeline(config)

    val mainFrameDF = nweRules.foldLeft(dataFrame)((df,op)=>{
      op.rule.foldLeft(df)((df,rule)=>{
        rule match{
          case DROP => DropColumn(df,op)
          case RENAME_COLUMN =>RenameColumn(df,op)
          case UUID_STRING => UUIDColumn(df,op)
          case EXTRACT_NESTED => ExtractNestedColumn(df,op)
          case ADD_COLUMN => AddColumn(df,op)
          case DATE_TIME => DateTime(df, op)
          case FILE_ARRIVAL_DATE_TIME => FileArrivalDateTime(df, op)
          case _ => df
        }
      })
    })

      val transformedDF = mainFrameDF
      .transform(AddLineDetail(config, sparkSession, cillJobRunDetails ))

    logger.info("Completed batch transformation  for logicalBatchId : {}", config.getString("logicalBatchId"))
    transformedDF
  }

  override def loadBatchData(config: Config, sparkSession: SparkSession, resultDataFrame: DataFrame): Unit = {

    val writePath = config.getString("baseWritePath")
    logger.info("Started writing transformed data for logicalBatchId : {} into path {}",
      config.getString("logicalBatchId"), writePath)

    val partnKeyYrHr = config.getString("staging.partnKeyYrHr")

    val partitionWritePath = getStagingPartitionPath(config, BASE_WRITE_PATH)
    val saveModeType = getPartitionSaveMode(config.getString("saveMode"),
      sparkSession.sparkContext.hadoopConfiguration, partitionWritePath)

    ORCWriter.write(dataFrame = resultDataFrame,
      path = writePath,
      partitionBy = Seq(partnKeyYrHr),
      saveMode = saveModeType)
    logger.info("Completed writing transformed data for logicalBatchId : {} into path {}",
      config.getString("logicalBatchId"), writePath)
  }

  override def postBatchProcessing(config: Config, spark: SparkSession, row: Row): Unit = {
    deleteCheckpoint(config, spark)
    logger.info("deleteCheckpoint executed successfully")
  }

  override def logBatchErrorDetails(config: Config, sparkSession: SparkSession, row: Row,
                                    sourceDataFrame: Option[DataFrame], throwable: Option[Throwable]): Unit = {
    try {
      throwable.foreach(exception => {
        val fileReadStrPath = config.getString("fileReadPath")
        val fileReadPath = new Path(fileReadStrPath)
        val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
        val fileErrorPath = new Path(config.getString("fileErrorPath"))
        val cillErrorLog: CillErrorLog = createCillErrorLog(config)

        if(fs.exists(new Path(fileReadStrPath)))
          FileUtil.copy(fs, fileReadPath, fs, fileErrorPath, false, sparkSession.sparkContext.hadoopConfiguration)

        val errorLogDf = ErrorLogGenerator.createErrorLogForJVMExp(sparkSession, config, exception, row, None)
        logger.info("Created error log")
        persistErrorLog(errorLogDf, config, cillErrorLog)
        logger.info("Persisted error log")

      })
    } catch {
      case e: Throwable =>
        logger.error(s"Exception occurred in logBatchErrorDetails: \n$e")
    }
  }

}







