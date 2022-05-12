package com.walmart.finance.cill.financialtransaction.rules

import com.cobrix.connector.rules.Transformer
import com.walmart.finance.cill.financialtransaction.base.CobrixTestSpec
import com.cobrix.connector.utility.Utils.BINReader
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}

class ExtractNestedColumnTest extends CobrixTestSpec{
  val readPath = "src/test/resources/cillCobrix/data/inputBinaryData/"
  val copybookPath = "src/test/resources/cillCobrix/data/copybook/WAPMSTR7.txt"

  implicit  val updatedConfig = config
  implicit val sparkSession = spark
  val inputData = BINReader(spark, config, readPath, copybookPath)

  "ExtractNestedColumnTest" should "extract nested columns when data type is not defined" in {
    val transformer = Transformer("WAP_VENDOR_X.WAP_VEN_NUM_X","vendor_number_x",List("extractNested"))
    val transformedData = ExtractNestedColumn(inputData,transformer)

    transformedData.columns should contain(transformer.target)
  }

  "ExtractNestedColumnTest" should "extract nested columns for StringType" in {
    val transformer = Transformer("WAP_VENDOR_X.WAP_VEN_NUM_X","vendor_number_x",List("extractNested"), Some("StringType"))
    val transformedData = ExtractNestedColumn(inputData,transformer)

    transformedData.select(col(transformer.target)).schema.head.dataType should be (StringType)
  }

  "ExtractNestedColumnTest" should "extract nested columns for IntegerType" in {
    val transformer = Transformer("FILLER_B.WAP_ALLOW_AMT","allowance_amount",List("extractNested"),Some("IntegerType"))
    val transformedData = ExtractNestedColumn(inputData,transformer)

    transformedData.select(col(transformer.target)).schema.head.dataType should be (IntegerType)
  }

  "ExtractNestedColumnTest" should "extract nested columns for DoubleType" in {
    val transformer = Transformer("FILLER_B.WAP_ALLOW_AMT","allowance_amount",List("extractNested"),Some("DoubleType"))
    val transformedData = ExtractNestedColumn(inputData,transformer)

    transformedData.select(col(transformer.target)).schema.head.dataType should be (DoubleType)
  }


}
