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

import com.hashmapinc.tempus.WitsmlObjects.v1411._
import com.hashmapinc.tempus.witsml.api.WitsmlVersion
import com.hashmapinc.tempus.witsml.client.WitsmlQuery
import de.kp.works.witsml.Objects1411._
import org.apache.spark.sql.DataFrame

class WitsmlObjects1411(
   /* Specifies the witsml.tcp address of the witsml server */
   endpoint:String,
   /* Specifies the username for Witsml Server */
   username:String,
   /* Specify the password for Witsml Server */
   password:String) extends WitsmlObjects(endpoint, username, password, WitsmlVersion.VERSION_1411) {

  def getObject(witsmlQuery: WitsmlQuery, objectType:Objects1411.Value):DataFrame = {
    try {

      val data = client.getObjectData(witsmlQuery)
      /*
       * Extract the XML representation
       */
      val xml1411 = data.getXmlOut
      if (xml1411 == null) return null

      val deserialized = objectType match {
        case ATTACHMENT =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjAttachments])
        case BHARUN =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjBhaRuns])
        case CEMENTJOB =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjCementJobs])
        case CHANGELOG =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjChangeLogs])
        case CONVCORE =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjConvCores])
        case DRILLREPORT =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjDrillReports])
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
        case OBJECTGROUP =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjObjectGroups])
        case OPSREPORT =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjOpsReports])
        case RIG =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjRigs])
        case RISK =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjRisks])
        case SIDEWALLCORE =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjSidewallCores])
        case STIMJOB =>
          witsmlMarshaller.deserialize(xml1411, classOf[ObjStimJobs])
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
        case _ =>
          throw new Exception(s"Provided object type `${objectType.toString}` is not supported.")
      }

      val json = mapper.writeValueAsString(deserialized)
      if (json == null) return null

      WitsmlTransformer.transform(json)


    } catch {
      case t:Throwable =>
        val message = s"Error getting data from WITSML server (Version 1411)."
        logger.error(message, t)

        null
    }
  }

  def getWells(witsmlQuery:WitsmlQuery):DataFrame = {

    try {
      val data = client.getObjectData(witsmlQuery)
      val xml = data.getXmlOut

      val deserialized = witsmlMarshaller.deserialize(xml, classOf[ObjWells])
      val json = mapper.writeValueAsString(deserialized)

      WitsmlTransformer.transform(json)

    } catch {
      case t:Throwable => null
    }

  }

  def getWellbores(witsmlQuery:WitsmlQuery):DataFrame = {

    try {
      val data = client.getObjectData(witsmlQuery)
      val xml = data.getXmlOut

      val deserialized = witsmlMarshaller.deserialize(xml, classOf[ObjWellbores])
      val json = mapper.writeValueAsString(deserialized)

      WitsmlTransformer.transform(json)

    } catch {
      case t:Throwable => null
    }

  }

}
