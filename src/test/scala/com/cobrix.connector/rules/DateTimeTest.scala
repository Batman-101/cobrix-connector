package com.walmart.finance.cill.financialtransaction.rules

import com.cobrix.connector.rules.Transformer
import com.typesafe.config.ConfigValueFactory
import com.walmart.finance.cill.financialtransaction.base.CobrixTestSpec
import com.cobrix.connector.utility.Utils.BINReader

class DateTimeTest extends CobrixTestSpec{
  val readPath = "src/test/resources/cillCobrix/data/inputBinaryData/"
  val copybookPath = "src/test/resources/cillCobrix/data/copybook/WAPMSTR7.txt"
  val inputData = BINReader(spark, config, readPath, copybookPath)

  implicit val updatedConfig = config
  implicit val sparkSession = spark

  val transformer = Transformer("","record_process_datetime",List("dateTime"),None,None,true)
  val transformedData = DateTime(inputData,transformer)

  transformedData.columns should contain(transformer.target)

}
