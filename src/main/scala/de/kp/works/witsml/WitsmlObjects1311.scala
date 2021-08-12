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

import com.hashmapinc.tempus.witsml.api.WitsmlVersion
import com.hashmapinc.tempus.witsml.client.WitsmlQuery
import de.kp.works.witsml.Objects1311._

object Objects1311 extends Enumeration {

  val BHARUN: Objects1311.Value             = Value("BHARUN")
  val CEMENTJOB: Objects1311.Value          = Value("CEMENTJOB")
  val CONVCORE: Objects1311.Value           = Value("CONVCORE")
  val DTSINSTALLEDSYSTEM: Objects1311.Value = Value("DTSINSTALLEDSYSTEM")
  val DTSMEASUREMENT: Objects1311.Value     = Value("DTSMEASUREMENT")
  val FLUIDREPORT: Objects1311.Value        = Value("FLUIDREPORT")
  val FORMATIONMARKER: Objects1311.Value    = Value("FORMATIONMARKER")
  val LOG: Objects1311.Value                = Value("LOG")
  val MESSAGE: Objects1311.Value            = Value("MESSAGE")
  val MUDLOG: Objects1311.Value             = Value("MUDLOG")
  val OPSREPORT: Objects1311.Value          = Value("OPSREPORT")
  val REALTIME: Objects1311.Value           = Value("REALTIME")
  val RIG: Objects1311.Value                = Value("RIG")
  val RISK: Objects1311.Value               = Value("RISK")
  val SIDEWALLCORE: Objects1311.Value       = Value("SIDEWALLCORE")
  val SURVEYPROGRAM: Objects1311.Value      = Value("SURVEYPROGRAM")
  val TARGET: Objects1311.Value             = Value("TARGET")
  val TRAJECTORY: Objects1311.Value         = Value("TRAJECTORY")
  val TUBULAR: Objects1311.Value            = Value("TUBULAR")
  val WBGEOMETRY: Objects1311.Value         = Value("WBGEOMETRY")
  val WELLLOG: Objects1311.Value            = Value("WELLLOG")

}

class WitsmlObjects1311(
  /* Specifies the witsml.tcp address of the witsml server */
  endpoint:String,
  /* Specifies the username for Witsml Server */
  username:String,
  /* Specify the password for Witsml Server */
  password:String) extends WitsmlObjects(endpoint, username, password, WitsmlVersion.VERSION_1311) {

  def getObject(witsmlQuery: WitsmlQuery, objectType:Objects1311.Value):Any = {
    try {
      objectType match {
        case BHARUN =>
          client.getBhaRunsAsObj(witsmlQuery)
        case CEMENTJOB =>
          client.getCementJobsAsObj(witsmlQuery)
        case CONVCORE =>
          client.getConvCoresAsObj(witsmlQuery)
        case DTSINSTALLEDSYSTEM =>
          client.getDtsInstalledSystemsAsObj(witsmlQuery)
        case DTSMEASUREMENT =>
          client.getDtsMeasurementsAsObj(witsmlQuery)
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
        case OPSREPORT =>
          client.getOpsReportsAsObj(witsmlQuery)
        case REALTIME =>
          client.getRealtimesAsObj(witsmlQuery)
        case RIG =>
          client.getRigsAsObj(witsmlQuery)
        case RISK =>
          client.getRisksAsObj(witsmlQuery)
        case SIDEWALLCORE =>
          client.getSideWallCoresAsObj(witsmlQuery)
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
        case WELLLOG =>
          client.getWellLogsAsObj(witsmlQuery)
        case _ =>
          throw new Exception(s"Provided object type `${objectType.toString}` is not supported.")
      }
    } catch {
      case t:Throwable =>
        val message = s"Error getting data from WITSML server (Version 1311)."
        logger.error(message, t)

        null
    }
  }

}

