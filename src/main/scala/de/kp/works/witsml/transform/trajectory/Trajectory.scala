package de.kp.works.witsml.transform.trajectory
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

import de.kp.works.witsml.transform.BaseTransform
import de.kp.works.witsml.WitsmlTransformer

import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions._

class Trajectory(data:ObjTrajectorys) extends BaseTransform {

  def transform(unpack:Boolean):DataFrame = {

    if (unpack) {
      val json = data.getTrajectory.map(mapper.writeValueAsString)
      WitsmlTransformer.transform(json)
    }
    else nested(data)

  }
  /*
   * A method to extract the first order
   * children of the data object
   */
  private def getTrajectory:List[ObjTrajectory] = {
    data.getTrajectory.toList
  }
}
