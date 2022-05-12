package com.walmart.finance.cill.financialtransaction.rules

import com.cobrix.connector.rules.Transformer
import com.walmart.finance.cill.financialtransaction.base.CobrixTestSpec
import com.cobrix.connector.utility.Utils.BINReader
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}

class AddColumnTest extends CobrixTestSpec{
  val readPath = "src/test/resources/cillCobrix/data/inputBinaryData/"
  val copybookPath = "src/test/resources/cillCobrix/data/copybook/WAPMSTR7.txt"
  val inputData = BINReader(spark, config, readPath, copybookPath)

  implicit val updatedConfig = config
  implicit val sparkSession = spark
  "AddColumnTest" should "add column when isDefaultNull equal to true" in {

    val transformer = Transformer("","auto_calculation_indicator",List("addColumn"),Some("StringType"),None, true)

    val transformedData = AddColumn(inputData,transformer)

    transformedData.columns should contain(transformer.target)
  }

  "AddColumnTest" should "add column when isDefaultNull equal to false and readFromConfig equal to false" in {

    val transformer = Transformer("","auto_calculation_indicator",List("addColumn"),Some("StringType"),None, false, false)

    val transformedData = AddColumn(inputData,transformer)

    transformedData.columns should contain(transformer.target)
  }

  "AddColumnTest" should "add column when isDefaultNull equal to false and readFromConfig equal to true" in {

    val transformer = Transformer("","auto_calculation_indicator",List("addColumn"),Some("StringType"),None, false, true, "auto_calculation_indicator")

    val transformedData = AddColumn(inputData,transformer)

    transformedData.select(col(transformer.target)).schema.head.dataType should be (StringType)
  }

  "AddColumnTest" should "add column when dataType is IntegerType" in {

    val transformer = Transformer("","auto_calculation_indicator",List("addColumn"), Some("IntegerType"), None, true)

    val transformedData = AddColumn(inputData,transformer)

    transformedData.select(col(transformer.target)).schema.head.dataType should be (IntegerType)
  }

  "AddColumnTest" should "add column when dataType is DoubleType" in {

    val transformer = Transformer("","auto_calculation_indicator",List("addColumn"), Some("DoubleType"), None, true)

    val transformedData = AddColumn(inputData,transformer)

    transformedData.select(col(transformer.target)).schema.head.dataType should be (DoubleType)
  }

  "AddColumnTest" should "add column when dataType is not defined" in {

    val transformer = Transformer("","auto_calculation_indicator",List("addColumn"), None, None, true)

    val transformedData = AddColumn(inputData,transformer)

    transformedData.columns should contain(transformer.target)
  }


}
