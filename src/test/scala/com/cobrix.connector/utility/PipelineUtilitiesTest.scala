package com.walmart.finance.cill.financialtransaction.utility

import com.cobrix.connector.utility.PipelineUtilities
import com.walmart.finance.cill.financialtransaction.base.CobrixTestSpec

class PipelineUtilitiesTest extends CobrixTestSpec {
  "PipelineUtilitiesTest" should "test configToPipeline" in {

    val ruleList = PipelineUtilities.configToPipeline(config)

    ruleList.size should equal(114)
  }

}

