/*
 * copyright 2017–present tomshley llc
 *
 * licensed under the apache license, version 2.0 (the "license");
 * you may not use this file except in compliance with the license.
 * you may obtain a copy of the license at
 *
 * http://www.apache.org/licenses/license-2.0
 *
 * unless required by applicable law or agreed to in writing, software
 * distributed under the license is distributed on an "as is" basis,
 * without warranties or conditions of any kind, either express or implied.
 * see the license for the specific language governing permissions and
 * limitations under the license.
 *
 * @author thomas schena @sgoggles <https://github.com/sgoggles> | <https://gitlab.com/sgoggles>
 *
 */

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
