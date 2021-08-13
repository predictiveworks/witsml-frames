package de.kp.works.witsml
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import java.io.StringReader
import java.util.logging.Logger
import javax.xml.bind.{JAXBContext, JAXBIntrospector, Unmarshaller}

class WitsmlMarshaller {

  private val LOG = Logger.getLogger(classOf[WitsmlMarshaller].getName)
  /**
   * Deserializes an WITSML XML string into an Object.
   * The object type is passed in via witsmlClass and the witsml
   * is passed in as a string.
   */
  def deserialize[T](witsml:String, witsmlClass:Class[T]):T = {

    val witsmlReader:StringReader = new StringReader(witsml)
    val jaxbContext:JAXBContext = JAXBContext.newInstance(witsmlClass)

    val jaxbUnmarshaller:Unmarshaller = jaxbContext.createUnmarshaller()
    JAXBIntrospector.getValue(jaxbUnmarshaller.unmarshal(witsmlReader)).asInstanceOf[T]

  }
}
