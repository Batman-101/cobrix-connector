package com.walmart.finance.cill.financialtransaction.rules

import com.cobrix.connector.rules.Transformer
import com.typesafe.config.ConfigValueFactory
import com.walmart.finance.cill.financialtransaction.base.CobrixTestSpec
import com.cobrix.connector.utility.Utils.BINReader

class FileArrivalDateTimeTest extends CobrixTestSpec{
  val readPath = "src/test/resources/cillCobrix/data/inputBinaryData/"
  val copybookPath = "src/test/resources/cillCobrix/data/copybook/WAPMSTR7.txt"
  val inputData = BINReader(spark, config, readPath, copybookPath)

  val updatedConfig = config.withValue("partitionReadPath",
    ConfigValueFactory.fromAnyRef("src/test/resources/cillCobrix/data/inputBinaryData/"))
  implicit val implConfig = updatedConfig
  implicit val sparkSession = spark

  val transformer = Transformer("","file_arrival_datetime",List("fileArrivalDateTime"),None,None,true)
  val transformedData = FileArrivalDateTime(inputData,transformer)

  transformedData.columns should contain(transformer.target)

}
