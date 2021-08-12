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

import com.hashmapinc.tempus.WitsmlObjects.v1411.{ObjWellbores, ObjWells}
import com.hashmapinc.tempus.witsml.api.WitsmlVersion
import com.hashmapinc.tempus.witsml.client.WitsmlQuery
import de.kp.works.witsml.Objects1411._

object Objects1411 extends Enumeration {
  val ATTACHMENT: Objects1411.Value         = Value("ATTACHMENT")
  val BHARUN: Objects1411.Value             = Value("BHARUN")
  val CEMENTJOB: Objects1411.Value          = Value("CEMENTJOB")
  val CHANGELOG: Objects1411.Value          = Value("CHANGELOG")
  val CONVCORE: Objects1411.Value           = Value("CONVCORE")
  val DRILLREPORT: Objects1411.Value        = Value("DRILLREPORT")
  val FLUIDREPORT: Objects1411.Value        = Value("FLUIDREPORT")
  val FORMATIONMARKER: Objects1411.Value    = Value("FORMATIONMARKER")
  val LOG: Objects1411.Value                = Value("LOG")
  val MESSAGE: Objects1411.Value            = Value("MESSAGE")
  val MUDLOG: Objects1411.Value             = Value("MUDLOG")
  val OBJECTGROUP: Objects1411.Value        = Value("OBJECTGROUP")
  val OPSREPORT: Objects1411.Value          = Value("OPSREPORT")
  val RIG: Objects1411.Value                = Value("RIG")
  val RISK: Objects1411.Value               = Value("RISK")
  val SIDEWALLCORE: Objects1411.Value       = Value("SIDEWALLCORE")
  val STIMJOB: Objects1411.Value            = Value("STIMJOB")
  val SURVEYPROGRAM: Objects1411.Value      = Value("SURVEYPROGRAM")
  val TARGET: Objects1411.Value             = Value("TARGET")
  val TRAJECTORY: Objects1411.Value         = Value("TRAJECTORY")
  val TUBULAR: Objects1411.Value            = Value("TUBULAR")
  val WBGEOMETRY: Objects1411.Value         = Value("WBGEOMETRY")

}

class WitsmlObjects1411(
   /* Specifies the witsml.tcp address of the witsml server */
   endpoint:String,
   /* Specifies the username for Witsml Server */
   username:String,
   /* Specify the password for Witsml Server */
   password:String) extends WitsmlObjects(endpoint, username, password, WitsmlVersion.VERSION_1411) {

  def getObject(witsmlQuery: WitsmlQuery, objectType:Objects1411.Value):Any = {
    try {
      objectType match {
        case ATTACHMENT =>
          client.getAttachmentsAsObj(witsmlQuery)
        case BHARUN =>
          client.getBhaRunsAsObj(witsmlQuery)
        case CEMENTJOB =>
          client.getCementJobsAsObj(witsmlQuery)
        case CHANGELOG =>
          client.getChangeLogsAsObj(witsmlQuery)
        case CONVCORE =>
          client.getConvCoresAsObj(witsmlQuery)
        case DRILLREPORT =>
          client.getDrillReportsAsObj(witsmlQuery)
        case FLUIDREPORT =>
          client.getFluidsReportsAsObj(witsmlQuery)
        case FORMATIONMARKER =>
          client.getFormationMarkersAsObj(witsmlQuery)
        case LOG =>
          client.getLogsAsObj(witsmlQuery)
        case MESSAGE =>
          client.getMessagesAsObj(witsmlQuery)
        case MUDLOG =>
          client.getMudLogsAsObj(witsmlQuery)
        case OBJECTGROUP =>
          client.getObjectGroupsAsObj(witsmlQuery)
        case OPSREPORT =>
          client.getOpsReportsAsObj(witsmlQuery)
        case RIG =>
          client.getRigsAsObj(witsmlQuery)
        case RISK =>
          client.getRisksAsObj(witsmlQuery)
        case SIDEWALLCORE =>
          client.getSideWallCoresAsObj(witsmlQuery)
        case STIMJOB =>
          client.getStimJobsAsObj(witsmlQuery)
        case SURVEYPROGRAM =>
          client.getSurveyProgramsAsObj(witsmlQuery)
        case TARGET =>
          client.getTargetsAsObj(witsmlQuery)
        case TRAJECTORY =>
          client.getTrajectorysAsObj(witsmlQuery)
        case TUBULAR =>
          client.getTubularsAsObj(witsmlQuery)
        case WBGEOMETRY =>
          client.getWbGeometrysAsObj(witsmlQuery)
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

  def getWells(witsmlQuery:WitsmlQuery):ObjWells = {

    try {
      /*
       * Note: Even if the server operates with version 1.3.11,
       * the server internally converts the result into version
       * v1411
       */
      val wells:com.hashmapinc.tempus.WitsmlObjects.v1411.ObjWells =
        client.getWellsAsObj(witsmlQuery)

      wells

    } catch {
      case t:Throwable => null
    }

  }

  def getWellbores(witsmlQuery:WitsmlQuery):ObjWellbores = {

    try {
      /*
       * Note: Even if the server operates with version 1.3.11,
       * the server internally converts the result into version
       * v1411
       */
      val wellbores:com.hashmapinc.tempus.WitsmlObjects.v1411.ObjWellbores =
        client.getWellboresForWellAsObj(witsmlQuery)

      wellbores

    } catch {
      case t:Throwable => null
    }

  }

}
