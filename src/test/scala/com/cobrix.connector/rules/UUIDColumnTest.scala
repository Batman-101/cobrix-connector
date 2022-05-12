package com.walmart.finance.cill.financialtransaction.rules

import com.cobrix.connector.rules.Transformer
import com.walmart.finance.cill.financialtransaction.base.CobrixTestSpec
import com.cobrix.connector.utility.Utils.BINReader

class UUIDColumnTest extends CobrixTestSpec{
  val readPath = "src/test/resources/cillCobrix/data/inputBinaryData/"
  val copybookPath = "src/test/resources/cillCobrix/data/copybook/WAPMSTR7.txt"

  implicit  val updatedConfig = config
  implicit val sparkSession = spark

  val inputData = BINReader(spark, config, readPath, copybookPath)

  "UUIDColumnTest" should "add uuid columns" in {
    val transformer = Transformer("", "id", List("uuid"))
    val transformedData = UUIDColumn(inputData, transformer)

    transformedData.columns should contain(transformer.target)
  }

  "UUIDColumnTest" should "generate unique uuid for each row" in {
    val transformer = Transformer("", "id", List("uuid"))
    val transformedData = UUIDColumn(inputData, transformer)

    transformedData.select(transformer.target).distinct().count() should be(transformedData.count())
  }

}
