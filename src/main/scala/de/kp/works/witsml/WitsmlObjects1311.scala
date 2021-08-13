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

import com.hashmapinc.tempus.WitsmlObjects.Util.WitsmlVersionTransformer
import com.hashmapinc.tempus.WitsmlObjects.v1411._
import com.hashmapinc.tempus.witsml.api.WitsmlVersion
import com.hashmapinc.tempus.witsml.client.WitsmlQuery
import de.kp.works.witsml.Objects1311._
import org.apache.spark.sql.DataFrame

class WitsmlObjects1311(
  /* Specifies the witsml.tcp address of the witsml server */
  endpoint:String,
  /* Specifies the username for Witsml Server */
  username:String,
  /* Specify the password for Witsml Server */
  password:String) extends WitsmlObjects(endpoint, username, password, WitsmlVersion.VERSION_1311) {
  /*
   * This transformer is usually part of the Witsml Client.
   * It is externalized here to enable message processing
   * that is compliant with Jackson v2.6.x
   */
  private val transform:WitsmlVersionTransformer =
    try {
      new WitsmlVersionTransformer()
    } catch {
      case t: Throwable => null
    }

  def getObject(witsmlQuery: WitsmlQuery, objectType:Objects1311.Value):DataFrame = {
    try {

      val data = client.getObjectData(witsmlQuery)
      /*
       * Extract the XML representation and convert into v1411
       * compliant XML format
       */
      val xml = data.getXmlOut

      val xml1411 = convertVersion(xml)
      if (xml1411 == null) return null

      val deserialized = objectType match {
        case BHARUN =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjBhaRuns])
        case CEMENTJOB =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjCementJobs])
        case CONVCORE =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjConvCores])
        case DTSINSTALLEDSYSTEM =>
          /*
           * Restricted to v1311
           */
          getDtsInstalledSystems(witsmlQuery)
        case DTSMEASUREMENT =>
         /*
          * Restricted to v1311
          */
        getDtsMeasurements(witsmlQuery)
        case FLUIDREPORT =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjFluidsReports])
        case FORMATIONMARKER =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjFormationMarkers])
        case LOG =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjLogs])
        case MESSAGE =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjMessages])
        case MUDLOG =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjMudLogs])
        case OPSREPORT =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjOpsReports])
        case REALTIME =>
          /*
           * Restricted to v1311
           */
          getRealtimes(witsmlQuery)
        case RIG =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjRigs])
        case RISK =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjRisks])
        case SIDEWALLCORE =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjSidewallCores])
        case SURVEYPROGRAM =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjSurveyPrograms])
        case TARGET =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjTargets])
        case TRAJECTORY =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjTrajectorys])
        case TUBULAR =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjTubulars])
        case WBGEOMETRY =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjWbGeometrys])
        case WELLLOG =>
          /*
           * Restricted to v1311
           */
          getWellLogs(witsmlQuery)
        case _ =>
          throw new Exception(s"Provided object type `${objectType.toString}` is not supported.")
      }

      val json = mapper.writeValueAsString(deserialized)
      if (json == null) return null

      WitsmlTransformer.transform(json)

    } catch {
      case t:Throwable =>
        val message = s"Error getting data from WITSML server (Version 1311)."
        logger.error(message, t)

        null
    }

  }

  def getDtsInstalledSystems(witsmlQuery:WitsmlQuery):DataFrame = {

    try {
      val data = client.getObjectData(witsmlQuery)
      val xml = data.getXmlOut

      val deserialized = witsmlMarshaller
        .deserialize(xml, classOf[com.hashmapinc.tempus.WitsmlObjects.v1311.ObjDtsInstalledSystems])
      val json = mapper.writeValueAsString(deserialized)

      WitsmlTransformer.transform(json)

    } catch {
      case t:Throwable => null
    }

  }

  def getDtsMeasurements(witsmlQuery:WitsmlQuery):DataFrame = {

    try {
      val data = client.getObjectData(witsmlQuery)
      val xml = data.getXmlOut

      val deserialized = witsmlMarshaller
        .deserialize(xml, classOf[com.hashmapinc.tempus.WitsmlObjects.v1311.ObjDtsMeasurements])
      val json = mapper.writeValueAsString(deserialized)

      WitsmlTransformer.transform(json)

    } catch {
      case t:Throwable => null
    }

  }

  def getRealtimes(witsmlQuery:WitsmlQuery):DataFrame = {

    try {
      val data = client.getObjectData(witsmlQuery)
      val xml = data.getXmlOut

      val deserialized = witsmlMarshaller.deserialize(xml,
        classOf[com.hashmapinc.tempus.WitsmlObjects.v1311.ObjRealtimes])
      val json = mapper.writeValueAsString(deserialized)

      WitsmlTransformer.transform(json)

    } catch {
      case t:Throwable => null
    }

  }

  def getWells(witsmlQuery:WitsmlQuery):DataFrame = {

    try {
      val data = client.getObjectData(witsmlQuery)
      val xml = data.getXmlOut

      val deserialized = witsmlMarshaller
        .deserialize(xml, classOf[com.hashmapinc.tempus.WitsmlObjects.v1311.ObjWells])
      val json = mapper.writeValueAsString(deserialized)

      WitsmlTransformer.transform(json)

    } catch {
      case t:Throwable => null
    }

  }

  def getWellLogs(witsmlQuery:WitsmlQuery):DataFrame = {

    try {
      val data = client.getObjectData(witsmlQuery)
      val xml = data.getXmlOut

      val deserialized = witsmlMarshaller
        .deserialize(xml, classOf[com.hashmapinc.tempus.WitsmlObjects.v1311.ObjWellLogs])
      val json = mapper.writeValueAsString(deserialized)

      WitsmlTransformer.transform(json)

    } catch {
      case t:Throwable => null
    }

  }

  def getWellbores(witsmlQuery:WitsmlQuery):DataFrame = {

    try {
      /*
       * The client interface method `getWellboresForWellAsObj`
       * cannot be used as the WitsmlMarshaller depends on the
       * Jackson version 2.9.9 which is not compliant with Spark
       */
      val data = client.getObjectData(witsmlQuery)
      /*
       * Extract the XML representation and convert into v1411
       * compliant XML format
       */
      val xml = data.getXmlOut

      val xml1411 = convertVersion(xml)
      if (xml1411 == null) return null
      /*
       * Transform XML representation into POJO representation
       */
      val deserialized = witsmlMarshaller.deserialize(xml1411, classOf[ObjWellbores])
      val json = mapper.writeValueAsString(deserialized)

      WitsmlTransformer.transform(json)

    } catch {
      case t:Throwable => null
    }

  }
  /**
   * This method is a copy of the respective method
   * with the Witsml Client; it is required for v1311
   * response only
   */
  private def convertVersion(original: String):String = {

    try {
      val converted = transform.convertVersion(original)
      if (converted.isEmpty) null else converted

    } catch {
      case t:Throwable => null
    }

  }
}

