package com.cobrix.connector.rules

import com.cobrix.connector.constants.ApplicationConstants
import com.cobrix.connector.utility.Utils
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, lit, split, substring, to_timestamp, to_utc_timestamp, udf}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, TimestampType}

case class Transformer(src:String, target:String, rule:List[String], datatype:Option[String]=None, default:Option[String]=None,
                       isDefaultNull:Boolean=false,
                       readFromConfig:Boolean= true,
                       configKey:String="")

trait Pipeline{
def apply(df:DataFrame,transformer: Transformer)(implicit config:Config, sparkSession: SparkSession): DataFrame
}

/** This extracts and populates the nested column data
 *
 * @param df            DataFrame
 * @param transformer   Transformer
 * @param config        typesafe config
 * @param sparkSession  SparkSession
 * @return DataFrame
 */
object ExtractNestedColumn extends Pipeline {
  override def apply(df: DataFrame, transformer: Transformer)(implicit config: Config, sparkSession: SparkSession): DataFrame = {
    val dataType = if(transformer.datatype.isDefined) {
      transformer.datatype.get match {
        case "DoubleType" => DoubleType
        case "IntegerType" => IntegerType
        case _ => StringType
      }
    }
    else df.select(transformer.src).schema.head.dataType

    df.withColumn(transformer.target,df(transformer.src).cast(dataType))
  }
}

/** This creates a new column and populates the column data
 *
 * @param df            DataFrame
 * @param transformer   Transformer
 * @param config        typesafe config
 * @param sparkSession  SparkSession
 * @return DataFrame
 */
object AddColumn extends Pipeline {
  override def apply(df: DataFrame, transformer: Transformer)(implicit config: Config, sparkSession: SparkSession): DataFrame = {

  val defaultValue = if (transformer.isDefaultNull) null
  else if(transformer.readFromConfig) config.getString(transformer.configKey)
  else transformer.default.getOrElse("")

    val dataType = if(transformer.datatype.isDefined) {
      transformer.datatype.get match {
        case "DoubleType" => DoubleType
        case "IntegerType" => IntegerType
        case "TimestampType" => TimestampType
        case _ => StringType
      }
    }
    else StringType

    df.withColumn(transformer.target,lit(defaultValue).cast(dataType))
  }
}

/** This drops the unwanted columns
 *
 * @param df            DataFrame
 * @param transformer   Transformer
 * @param config        typesafe config
 * @param sparkSession  SparkSession
 * @return DataFrame
 */
object DropColumn extends Pipeline{
  override def apply(df: DataFrame, transformer: Transformer)(implicit config: Config, sparkSession: SparkSession): DataFrame = {
    df.drop(transformer.src)
  }
}

/** This extracts and populates the header level column data
 *
 * @param df            DataFrame
 * @param transformer   Transformer
 * @param config        typesafe config
 * @param sparkSession  SparkSession
 * @return DataFrame
 */
object RenameColumn extends Pipeline{
  override def apply(df: DataFrame, transformer: Transformer)(implicit config: Config, sparkSession: SparkSession): DataFrame = {
    val dataType = if(transformer.datatype.isDefined) {
      transformer.datatype.get match {
        case "DoubleType" => DoubleType
        case "IntegerType" => IntegerType
        case "LongType" => LongType
        case _ => StringType
      }
    }
    else StringType

    val dataTypeDF = df.withColumnRenamed(transformer.src,transformer.target)
    dataTypeDF.withColumn(transformer.target, dataTypeDF.col(transformer.target).cast(dataType))
  }
}

/** This creates and populates the UUID column data
 *
 * @param df            DataFrame
 * @param transformer   Transformer
 * @param config        typesafe config
 * @param sparkSession  SparkSession
 * @return DataFrame
 */
object UUIDColumn extends Pipeline{
  override def apply(df: DataFrame, transformer: Transformer)(implicit config: Config, sparkSession: SparkSession): DataFrame = {
    val uuid = udf(() => java.util.UUID.randomUUID().toString)
    df.withColumn(transformer.target, uuid())
  }
}

/** This creates and populates the DateTime column data
 *
 * @param df            DataFrame
 * @param transformer   Transformer
 * @param config        typesafe config
 * @param sparkSession  SparkSession
 * @return DataFrame
 */
object DateTime extends Pipeline{
  override def apply(df: DataFrame, transformer: Transformer)(implicit config: Config, sparkSession: SparkSession): DataFrame = {
    df.withColumn(transformer.target,current_timestamp())
  }
}

/** This creates and populates the File Arrival DateTime column data
 *
 * @param df            DataFrame
 * @param transformer   Transformer
 * @param config        typesafe config
 * @param sparkSession  SparkSession
 * @return DataFrame
 */
object FileArrivalDateTime extends Pipeline{
  override def apply(df: DataFrame, transformer: Transformer)(implicit config: Config, sparkSession: SparkSession): DataFrame = {

    val dateUdf = udf(Utils.dateTimeinMiliConvertor _)
    df.withColumn(transformer.target, to_utc_timestamp(
      dateUdf(substring(split(col(ApplicationConstants.INPUT_FILE_NAME), "TfrDt_").getItem(1), 0, 23)).cast(TimestampType),
      "CST"))

  }
}
