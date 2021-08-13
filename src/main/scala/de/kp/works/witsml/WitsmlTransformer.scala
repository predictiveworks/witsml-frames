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

import de.kp.works.spark.Session
import org.apache.spark.sql.DataFrame

object WitsmlTransformer {

  private val session = Session.getSession
  import session.implicits._

  def transform(json:String):DataFrame = {
    val dataset = Seq(json).toDS()
    session.read.json(dataset)
  }

  def transform(data:Seq[String]):DataFrame = {
    val dataset = data.toDS()
    session.read.json(dataset)

  }
}
