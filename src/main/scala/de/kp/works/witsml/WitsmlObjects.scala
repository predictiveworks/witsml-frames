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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.hashmapinc.tempus.WitsmlObjects.Util.WitsmlVersionTransformer
import com.hashmapinc.tempus.witsml.api.WitsmlVersion
import com.hashmapinc.tempus.witsml.client.{Client, WitsmlQuery}
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

abstract class WitsmlObjects(
  /* Specifies the witsml.tcp address of the witsml server */
  endpoint:String,
  /* Specifies the username for Witsml Server */
  username:String,
  /* Specify the password for Witsml Server */
  password:String,
  version:WitsmlVersion) {

  protected val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  protected val witsmlMarshaller = new WitsmlMarshaller()

  protected val logger: Logger = LoggerFactory.getLogger(classOf[WitsmlObjects])

  protected val client:Client = new Client(endpoint)
  client.setUserName(username)

  client.setPassword(password)
  client.setVersion(version)

  client.connect()
  /**
   * This transformer is usually part of the Witsml Client.
   * It is externalized here to enable message processing
   * that is compliant with Jackson v2.6.x
   */
  protected val transform:WitsmlVersionTransformer =
    try {
      new WitsmlVersionTransformer()
    } catch {
      case t: Throwable => null
    }

  /**
   * This method is a copy of the respective method
   * with the Witsml Client; it is required for v1311
   * response only
   */
  protected def convertVersion(original: String):String = {

    try {
      val converted = transform.convertVersion(original)
      if (converted.isEmpty) null else converted

    } catch {
      case t:Throwable => null
    }

  }

  protected def extract1311[T](witsmlQuery:WitsmlQuery, witsmlClass:Class[T]):T = {

    val data = client.getObjectData(witsmlQuery)
    val xml = data.getXmlOut

    val xml1411 = convertVersion(xml)
    witsmlMarshaller.deserialize(xml1411, classOf[T])

  }

  protected def extract1411[T](witsmlQuery:WitsmlQuery, witsmlClass:Class[T]):T = {

    val data = client.getObjectData(witsmlQuery)
    val xml = data.getXmlOut

    witsmlMarshaller.deserialize(xml, classOf[T])

  }

  protected def nested(deserialized:AnyRef):DataFrame = {
    val json = mapper.writeValueAsString(deserialized)
    WitsmlTransformer.transform(json)
  }

}
