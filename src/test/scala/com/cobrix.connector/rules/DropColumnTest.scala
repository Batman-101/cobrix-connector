package com.walmart.finance.cill.financialtransaction.rules

import com.cobrix.connector.rules.Transformer
import com.walmart.finance.cill.financialtransaction.base.CobrixTestSpec
import com.cobrix.connector.utility.Utils.BINReader

class DropColumnTest extends CobrixTestSpec{
  val readPath = "src/test/resources/cillCobrix/data/inputBinaryData/"
  val copybookPath = "src/test/resources/cillCobrix/data/copybook/WAPMSTR7.txt"

  implicit val updatedConfig = config
  implicit val sparkSession = spark

  val inputData = BINReader(spark, config, readPath, copybookPath)

  val transformer = Transformer("WAP_VENDOR_BREAK","",List("drop"))

  val transformedData = DropColumn(inputData,transformer)

  transformedData.columns should not contain(transformer.src)

}
