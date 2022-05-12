package com.walmart.finance.cill.financialtransaction.utility

import com.cobrix.connector.utility.Utils
import com.typesafe.config.{Config, ConfigValueFactory}
import com.walmart.finance.cill.financialtransaction.base.CobrixTestSpec
import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.io.File

class UtilsTest extends CobrixTestSpec{
  private val latestBasePath = "src/test/resources/SampleForTestCaseItShouldAutoDelete/"

  def mkdirs(path: List[String]): Boolean =
    path.tail.foldLeft(new File(path.head)) {
      (baseDir: File, name: String) => baseDir.mkdir; new File(baseDir, name)
    }.mkdir

  mkdirs(List(latestBasePath, "2021", "01", "02"))
  mkdirs(List(latestBasePath, "2021", "01", "04"))
  mkdirs(List(latestBasePath, "2021", "01", "05"))

  "Utils.updateConfig " should "update the baseWritePath from row" in {
    val cillSourceWritePathColumn = config.getString("cillSourceWritePathColumn")
    val cillSourceReadPathColumn = config.getString("cillSourceReadPathColumn")

    val someSchema = List(
      StructField("sink_partition_path", StringType, nullable = true),
      StructField("logical_batch_id", StringType, nullable = true),
      StructField(cillSourceWritePathColumn, StringType, nullable = true),
      StructField(cillSourceReadPathColumn, StringType, nullable = true),
      StructField("source_system", StringType, nullable = true),
      StructField("origin_feed_file", StringType, nullable = true)
    )
    val row = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row("partition", "some_id", "some/write/path", "some/in/path", "someSourceSystem", "someOriginFeedFile"))), StructType(someSchema)).take(1)(0)
    val updatedConfig = Utils.updateConfig(
      config.withValue("basePath", ConfigValueFactory.fromAnyRef("")), row)

    updatedConfig.getString("baseWritePath") should be("/some/write/path")
    updatedConfig.getString("errorPath") should be("/some/error/path")
    updatedConfig.getString("logicalBatchId") should be("some_id")
    updatedConfig.getString("baseReadPath") should be("/some/in/path")
    updatedConfig.getString("partitionReadPath") should be("/some/in/path/partition")
    updatedConfig.getString("sourcePartitionPath") should be("partition")
    updatedConfig.getString("sinkPartitionPath") should be("partition")
    updatedConfig.getString("sourceSystem") should be("someSourceSystem")
    updatedConfig.getString("originFeedFile") should be("someOriginFeedFile")
  }

  private val partitionWritePath = (config : Config) => s"${config.getString("writePath")}/${config.getString(s"staging.partnKeyYrHr")}=${config.getString("partnKeyYrHr")}"

  "Utils.getSaveMode configuration for errorIfExists " should " SaveMode Append" in {
    val updatedConfig = config.withValue("partnKeyYrHr", ConfigValueFactory.fromAnyRef("19000101"))
    Utils.getPartitionSaveMode(updatedConfig.getString("saveMode"), spark.sparkContext.hadoopConfiguration, partitionWritePath(updatedConfig)) should be(SaveMode.Append)
  }

  "Utils.getSaveMode configuration for Overwrite " should " SaveMode Append" in {
    val updatedConfig = config.withValue("saveMode", ConfigValueFactory.fromAnyRef("Overwrite"))
    Utils.getPartitionSaveMode(updatedConfig.getString("saveMode"), spark.sparkContext.hadoopConfiguration, partitionWritePath(updatedConfig)) should be(SaveMode.Append)
  }

  "Utils.getSaveMode " should " be Append" in {
    val updatedConfig = config.withValue("saveMode", ConfigValueFactory.fromAnyRef("Append"))
    Utils.getPartitionSaveMode(updatedConfig.getString("saveMode"), spark.sparkContext.hadoopConfiguration, partitionWritePath(updatedConfig)) should be(SaveMode.Append)
  }

  "Utils.getSaveMode " should "throw FileAlreadyExistsException " in {
    mkdirs(List(latestBasePath, "partn_key_yr_hr=19000101"))
    val updatedConfig = config
      .withValue("writePath", ConfigValueFactory.fromAnyRef(latestBasePath))
      .withValue("partnKeyYrHr", ConfigValueFactory.fromAnyRef("19000101"))
      .withValue("saveMode", ConfigValueFactory.fromAnyRef("errorifexists"))
    assertThrows[FileAlreadyExistsException] {
      Utils.getPartitionSaveMode(updatedConfig.getString("saveMode"), spark.sparkContext.hadoopConfiguration, partitionWritePath(updatedConfig))
    }
  }

  "Utils.getSaveMode " should "throw IllegalArgumentException " in {
    val updatedConfig = config.withValue("saveMode", ConfigValueFactory.fromAnyRef("Ignore"))
    assertThrows[IllegalArgumentException] {
      Utils.getPartitionSaveMode(updatedConfig.getString("saveMode"), spark.sparkContext.hadoopConfiguration, partitionWritePath(updatedConfig))
    }
  }

  "Utils.getStagingPartitionPath " should "test getStagingPartitionPath method" in {
    val updatedConfig = config.withValue("baseWritePath", ConfigValueFactory.fromAnyRef("/some/base/write/path/"))
    val writePath = Utils.getStagingPartitionPath(updatedConfig, "baseWritePath")

    writePath should equal("/some/base/write/path/partn_key_yr_hr=2021031220")
  }

  "Utils.getStagingPartitionPath " should "create cill job run details" in {
    val updatedConfig = config.withValue("wfName", ConfigValueFactory.fromAnyRef("wf1"))
      .withValue("processName", ConfigValueFactory.fromAnyRef("pipe1"))
      .withValue("sourceSystem", ConfigValueFactory.fromAnyRef("i0089"))
      .withValue("processingStage", ConfigValueFactory.fromAnyRef("staging"))
      .withValue("baseReadPath", ConfigValueFactory.fromAnyRef("/some/base/read/path/"))
      .withValue("baseWritePath", ConfigValueFactory.fromAnyRef("/some/base/write/path/"))
      .withValue("errorMetaDataPath", ConfigValueFactory.fromAnyRef("/some/base/error/meta/data/path/"))
    val cillJobRun = Utils.createCillJobRun(updatedConfig)

    cillJobRun.workFlowName should equal("wf1")
    cillJobRun.processName should equal("pipe1")
    cillJobRun.sourceSystem should equal("i0089")
    cillJobRun.processingStage should equal("staging")

  }

  "Utils.getFileArrivalDateTime " should "fetch the file arrival date time" in {
    val updatedConfig = config.withValue("partitionReadPath", ConfigValueFactory.fromAnyRef("src/test/resources/cillCobrix/data/inputBinaryData/"))
    Utils.getFileArrivalDateTime(spark, updatedConfig)
  }

  "Utils.renameGZFile " should "rename bin.gz files to .bin file" in {
    val updatedConfig = config.withValue("partitionReadPath", ConfigValueFactory.fromAnyRef("src/test/resources/cillCobrix/data/compressBinFile/"))
    .withValue("originFeedFile", ConfigValueFactory.fromAnyRef("N013.FIN.GFUS696.I0089.WRKBKUP.G9236V00_FileDt_2021-05-06_TfrDt_2021-05-06-19.25.07.342610.bin.gz"))
        .withValue("fileReadPath", ConfigValueFactory.fromAnyRef("cillCobrix/data/compressBinFile/N013.FIN.GFUS696.I0089.WRKBKUP.G9236V00_F"))
    Utils.renameGZFile(updatedConfig, spark).toString should be ("cillCobrix/data/compressBinFile/N013.FIN.GFUS696.I0089.WRKBKUP.G9236V00_F")
  }

}
