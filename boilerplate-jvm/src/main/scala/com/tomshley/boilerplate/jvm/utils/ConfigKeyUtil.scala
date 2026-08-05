package com.tomshley.boilerplate.jvm.utils

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.typed.ActorSystem

trait ConfigKeyUtil {
  def config(using systemOption: Option[ActorSystem[?]] = Option.empty[ActorSystem[?]]): Config = {
    systemOption match
      case Some(value) => value.settings.config
      case None =>
        ConfigFactory
          .load()
  }
  def getValueWithDefault[T](keyName:String, defaultValue:Option[T]) : Option[T] = {
    if (config.hasPathOrNull(keyName)) {     
      if (config.getIsNull(keyName)) {  
        defaultValue     
      } else {  
        Some(config.getValue(keyName).unwrapped().asInstanceOf[T])     
      } 
    } else {
      None
    }
  }
}
