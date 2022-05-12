package com.cobrix.connector.utility

import com.cobrix.connector.rules.Transformer
import com.typesafe.config.Config
import com.cobrix.connector.constants.ApplicationConstants.DROP

object PipelineUtilities {

  /** This method sets the Transformer case class value based on the rule in config
   *
   * @param config           typesafe config
   * @return List[Transformer]
   */
  def configToPipeline(config:Config):List[Transformer]={
    val rulesBuffer = scala.collection.mutable.ListBuffer[Transformer]()
    val path = "rules"

    config.getConfig(path).root().entrySet().asScala.foreach(entry=>{
      val colName = entry.getKey
      val values = config.getObject(s"$path.$colName").toConfig
      val rules = getOrDefaultList(values,"transform")
      val transfomer = Transformer(
        getOrDefault(values,"src"),
        getOrDefault(values,"target"),
        rules,
        getOrDefaultDataType(values,"datatype"),
        Option(getOrDefault(values,"default")),
        getOrDefaultBoolean(values,"is_default_null"),
        getOrDefaultBoolean(values,"read_from_config"),
        getOrDefault(values,"configKey"))

      if(rules.contains(DROP)) rulesBuffer.append(transfomer)
      else rulesBuffer.prepend(transfomer)

    })
    rulesBuffer.toList
  }

  /** This method gets the default value
   *
   * @param config         typesafe config
   * @param key            default key
   * @param default        default value
   * @return String
   */
  def getOrDefault(config:Config,key:String,default:String=""):String={
    Try(config.getString(key)).getOrElse(default)
  }

  /** This method gets the default boolean value
   *
   * @param config         typesafe config
   * @param key            default key
   * @param default        default value
   * @return Boolean
   */
  def getOrDefaultBoolean(config:Config,key:String,default:Boolean=false):Boolean={
    Try(config.getBoolean(key)).getOrElse(default)
  }

  /** This method gets the default List
   *
   * @param config         typesafe config
   * @param key            default key
   * @param default        default value
   * @return List[String]
   */
  def getOrDefaultList(config:Config,key:String,default:List[String] = List.empty[String]):List[String]={
    Try(config.getString(key).split(",").toList).getOrElse(default)
  }

  /** This method gets the default DataType
   *
   * @param config         typesafe config
   * @param key            default key
   * @param default        default value
   * @return Option[String]
   */
  def getOrDefaultDataType(config:Config,key:String,default:Option[String]=None): Option[String]={
    Try(Some(config.getString(key))).getOrElse(default)
  }

}
