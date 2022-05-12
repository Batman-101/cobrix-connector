package com.walmart.finance.cill.financialtransaction.rules

import com.cobrix.connector.rules.Transformer
import com.walmart.finance.cill.financialtransaction.base.CobrixTestSpec
import com.cobrix.connector.utility.Utils.BINReader
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}



class RenameColumnTest extends CobrixTestSpec {
  val readPath = "src/test/resources/cillCobrix/data/inputBinaryData/"
  val copybookPath = "src/test/resources/cillCobrix/data/copybook/WAPMSTR7.txt"

  implicit val updatedConfig = config
  implicit val sparkSession = spark

  val inputData = BINReader(spark, config, readPath, copybookPath)

  "RenameColumnTest" should "rename column when data type is StringType" in {
    val transformer = Transformer("WAP_VENDOR","vendor_id",List("renameColumn"), Some("StringType"))

    val transformedData = RenameColumn(inputData,transformer)

    transformedData.select(col(transformer.target)).schema.head.dataType should be (StringType)
  }

  "RenameColumnTest" should "rename column when data type is IntegerType" in {
    val transformer = Transformer("WAP_VENDOR","vendor_id",List("renameColumn"), Some("IntegerType"))

    val transformedData = RenameColumn(inputData,transformer)

    transformedData.select(col(transformer.target)).schema.head.dataType should be (IntegerType)
  }

  "RenameColumnTest" should "rename column when data type is DoubleType" in {
    val transformer = Transformer("WAP_VENDOR","vendor_id",List("renameColumn"), Some("DoubleType"))

    val transformedData = RenameColumn(inputData,transformer)

    transformedData.select(col(transformer.target)).schema.head.dataType should be (DoubleType)
  }

  "RenameColumnTest" should "rename column when data type is LongType" in {
    val transformer = Transformer("WAP_VENDOR","vendor_id",List("renameColumn"), Some("LongType"))

    val transformedData = RenameColumn(inputData,transformer)

    transformedData.select(col(transformer.target)).schema.head.dataType should be (LongType)
  }

  "RenameColumnTest" should "rename column when data type is not defined" in {
    val transformer = Transformer("WAP_VENDOR","vendor_id",List("renameColumn"))

    val transformedData = RenameColumn(inputData,transformer)

    transformedData.columns should contain(transformer.target)
  }

}
