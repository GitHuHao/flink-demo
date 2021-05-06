/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bigdata.flink.proj.testing.taxi.scala
import com.bigdata.flink.proj.taxi.app.{HourlyTipsExercise, HourlyTipsSolution}
import com.bigdata.flink.proj.testing.base.{TestFareSource, TestSink, Testable}

import java.util

class HourlyTipsTest extends com.bigdata.flink.proj.testing.taxi.HourlyTipsTest {
  private val scalaExercise: Testable = () => HourlyTipsExercise.main(Array.empty[String])

  @throws[Exception]
  override protected def results(source: TestFareSource): util.List[Tuple3[java.lang.Long, java.lang.Long, java.lang.Float]] = {
    val scalaSolution: Testable = () => HourlyTipsSolution.main(Array.empty[String])
    val tuples: util.List[_] = runApp(source, new TestSink[Tuple3[java.lang.Long, java.lang.Long, java.lang.Float]], scalaExercise, scalaSolution)
    javaTuples(tuples.asInstanceOf[util.List[(Long, Long, Float)]])
  }

  private def javaTuples(a: util.List[(Long, Long, Float)]): util.ArrayList[Tuple3[java.lang.Long, java.lang.Long, java.lang.Float]] = {
    val javaCopy: util.ArrayList[Tuple3[java.lang.Long, java.lang.Long, java.lang.Float]] = new util.ArrayList[Tuple3[java.lang.Long, java.lang.Long, java.lang.Float]](a.size)
    a.iterator.forEachRemaining((t: (Long, Long, Float)) => javaCopy.add(Tuple3.apply(t._1, t._2, t._3)))
    javaCopy
  }

}
