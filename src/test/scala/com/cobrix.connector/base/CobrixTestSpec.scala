package com.walmart.finance.cill.financialtransaction.base

import org.scalatest.matchers.should.Matchers
import com.typesafe.config.{Config, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import com.walmart.finance.cill.common.config.SparkSessionInitializer
import com.walmart.finance.cill.common.utilities.ConfigUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, ParallelTestExecution}
import org.scalatest.flatspec.AnyFlatSpec

abstract class CobrixTestSpec extends AnyFlatSpec with BeforeAndAfterAll with BeforeAndAfterEach
  with Matchers with ParallelTestExecution with LazyLogging {
  val testDirectory = "target/testCaseRunDir"
  val properties: Array[String] = Array("-DconfigFile=cobrixTest.conf"
    ,"-Denvironment=cillCobrix/env/localTest.conf")

  val baseConfig: Config = ConfigUtils.getConfiguration(properties)
  val partnKeyYrHrBaseTest = baseConfig.getString("staging.partnKeyYrHr")
  val partnKeyYrHrValueBaseTest = baseConfig.getLong("partnKeyYrHr")
  val config: Config = baseConfig.withValue("sourcePartitionPath",
    ConfigValueFactory.fromAnyRef(partnKeyYrHrBaseTest+"="+partnKeyYrHrValueBaseTest))
  lazy val spark: SparkSession = SparkSessionInitializer.initializeSparkSession(config)

  override def afterEach(): Unit = {
    spark.catalog.clearCache()
  }

}
