package org.tianlangstudio.data.hamal

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Created by zhuhq on 2016/4/28.
 */
class DataxConf(fileName:String = Constants.DATAX_MASTER_CONF_NAME) {

  val conf = ConfigFactory.load(fileName).withFallback(ConfigFactory.load())
  def withFallback(other:Config) = {
      conf.withFallback(other)
  }
  def getInt(name:String,defVal:Int) = {

    if(conf.hasPath(name)) {
      conf.getInt(name)
    }else {
      defVal
    }

  }

  def getString(name:String,defVal:String) = {
    if(conf.hasPath(name)) {
      conf.getString(name)
    }else {
      defVal
    }
  }
  def getString(name:String) = {
    conf.getString(name)
  }
  def getInt(name:String) = {
    conf.getInt(name)

  }
  def getBoolean(name:String,defVal:Boolean) = {
    if(conf.hasPath(name)) {
      conf.getBoolean(name)
    }else {
      defVal
    }
  }
  def getBoolean(name:String) = {
    conf.getBoolean(name)
  }
  def getConf = {
    conf
  }

}
