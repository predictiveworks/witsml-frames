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
import de.kp.works.witsml.transform.trajectory.Trajectory
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions._

class WitsmlObjects1411(
   /* Specifies the witsml.tcp address of the witsml server */
   endpoint:String,
   /* Specifies the username for Witsml Server */
   username:String,
   /* Specify the password for Witsml Server */
   password:String) extends WitsmlObjects(endpoint, username, password, WitsmlVersion.VERSION_1411) {

  def transform(witsmlQuery: WitsmlQuery, objectType:Objects1411.Value, unpack:Boolean = true):DataFrame = {
    try {
     objectType match {
        case ATTACHMENT =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjAttachments])
          if (unpack) {
            val json = deserialized.getAttachment.map(mapper.writeValueAsString)
            WitsmlTransformer.transform(json)
          }
          else nested(deserialized)
        case BHARUN =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjBhaRuns])
          if (unpack) {
            val json = deserialized.getBhaRun.map(mapper.writeValueAsString)
            WitsmlTransformer.transform(json)
          }
          else nested(deserialized)
        case CEMENTJOB =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjCementJobs])
          if (unpack) {
            val json = deserialized.getCementJob.map(mapper.writeValueAsString)
            WitsmlTransformer.transform(json)
          }
          else nested(deserialized)
        case CHANGELOG =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjChangeLogs])
          if (unpack) {
            val json = deserialized.getChangeLog.map(mapper.writeValueAsString)
            WitsmlTransformer.transform(json)
          }
          else nested(deserialized)
        case CONVCORE =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjConvCores])
          if (unpack) {
            val json = deserialized.getConvCore.map(mapper.writeValueAsString)
            WitsmlTransformer.transform(json)
          }
          else nested(deserialized)
        case DRILLREPORT =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjDrillReports])
          if (unpack) {
            val json = deserialized.getDrillReport.map(mapper.writeValueAsString)
            WitsmlTransformer.transform(json)
          }
          else nested(deserialized)
        case FLUIDREPORT =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjFluidsReports])
          if (unpack) {
            val json = deserialized.getFluidsReport.map(mapper.writeValueAsString)
            WitsmlTransformer.transform(json)
          }
          else nested(deserialized)
        case FORMATIONMARKER =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjFormationMarkers])
          if (unpack) {
            val json = deserialized.getFormationMarker.map(mapper.writeValueAsString)
            WitsmlTransformer.transform(json)
          }
          else nested(deserialized)
        case LOG =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjLogs])
          if (unpack) {
            val json = deserialized.getLog.map(mapper.writeValueAsString)
            WitsmlTransformer.transform(json)
          }
          else nested(deserialized)
        case MESSAGE =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjMessages])
          if (unpack) {
            val json = deserialized.getMessage.map(mapper.writeValueAsString)
            WitsmlTransformer.transform(json)
          }
          else nested(deserialized)
        case MUDLOG =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjMudLogs])
          if (unpack) {
            val json = deserialized.getMudLog.map(mapper.writeValueAsString)
            WitsmlTransformer.transform(json)
          }
          else nested(deserialized)
        case OBJECTGROUP =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjObjectGroups])
          if (unpack) {
            val json = deserialized.getObjectGroup.map(mapper.writeValueAsString)
            WitsmlTransformer.transform(json)
          }
          else nested(deserialized)
        case OPSREPORT =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjOpsReports])
          if (unpack) {
            val json = deserialized.getOpsReport.map(mapper.writeValueAsString)
            WitsmlTransformer.transform(json)
          }
          else nested(deserialized)
        case RIG =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjRigs])
          if (unpack) {
            val json = deserialized.getRig.map(mapper.writeValueAsString)
            WitsmlTransformer.transform(json)
          }
          else nested(deserialized)
        case RISK =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjRisks])
          if (unpack) {
            val json = deserialized.getRisk.map(mapper.writeValueAsString)
            WitsmlTransformer.transform(json)
          }
          else nested(deserialized)
        case SIDEWALLCORE =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjSidewallCores])
          if (unpack) {
            val json = deserialized.getSidewallCore.map(mapper.writeValueAsString)
            WitsmlTransformer.transform(json)
          }
          else nested(deserialized)
        case STIMJOB =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjStimJobs])
          if (unpack) {
            val json = deserialized.getStimJob.map(mapper.writeValueAsString)
            WitsmlTransformer.transform(json)
          }
          else nested(deserialized)
        case SURVEYPROGRAM =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjSurveyPrograms])
          if (unpack) {
            val json = deserialized.getSurveyProgram.map(mapper.writeValueAsString)
            WitsmlTransformer.transform(json)
          }
          else nested(deserialized)
        case TARGET =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjTargets])
          if (unpack) {
            val json = deserialized.getTarget.map(mapper.writeValueAsString)
            WitsmlTransformer.transform(json)
          }
          else nested(deserialized)
        case TRAJECTORY =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjTrajectorys])
          val transformer = new Trajectory(deserialized)
          transformer.transform(unpack)
        case TUBULAR =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjTubulars])
          if (unpack) {
            val json = deserialized.getTubular.map(mapper.writeValueAsString)
            WitsmlTransformer.transform(json)
          }
          else nested(deserialized)
        case WBGEOMETRY =>
          val deserialized = extract1411(witsmlQuery, classOf[ObjWbGeometrys])
          if (unpack) {
            val json = deserialized.getWbGeometry.map(mapper.writeValueAsString)
            WitsmlTransformer.transform(json)
          }
          else nested(deserialized)
        case _ =>
          throw new Exception(s"Provided object type `${objectType.toString}` is not supported.")
      }

    } catch {
      case t:Throwable =>
        val message = s"Error getting data from WITSML server (Version 1411)."
        logger.error(message, t)

        null
    }
  }

  def getWells(witsmlQuery:WitsmlQuery,unpack:Boolean=true):DataFrame = {

    try {

      val deserialized:ObjWells = extract1411(witsmlQuery, classOf[ObjWells])
      if (unpack) {
        /*
         * List of (nested) JSON object
         */
        val json = deserialized.getWell.map(mapper.writeValueAsString)
        WitsmlTransformer.transform(json)

      } else nested(deserialized)

    } catch {
      case t:Throwable => null
    }

  }

  def getWellbores(witsmlQuery:WitsmlQuery,unpack:Boolean=true):DataFrame = {

    try {

      val deserialized = extract1411(witsmlQuery, classOf[ObjWellbores])
      if (unpack) {
        /*
         * List of (nested) JSON object
         */
        val json = deserialized.getWellbore.map(mapper.writeValueAsString)
        WitsmlTransformer.transform(json)

      } else nested(deserialized)

    } catch {
      case t:Throwable => null
    }

  }
}
