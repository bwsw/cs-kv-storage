// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.bwsw.cloudstack.storage.kv.mock

import akka.actor.Actor
import com.bwsw.cloudstack.storage.kv.mock.MockActor.Expectation
import org.scalatest.Matchers

import scala.collection.mutable

class MockActor() extends Actor with Matchers {

  private val queue = mutable.Queue.empty[Expectation]

  override def receive: Receive = {
    case message: Any =>
      if (queue.isEmpty) {
        fail("Unexpected message " + message)
      }
      val expectation = queue.dequeue()
      message should equal(expectation.message)
      sender ! expectation.responseProducer.apply()
  }

  def expect(expectation: Expectation): Unit = queue.enqueue(expectation)

  def clearAndExpect(expectation: Expectation): Unit = {
    clear()
    expect(expectation)
  }

  def clear(): Unit = queue.clear()
}

object MockActor {

  case class Expectation(message: Any, responseProducer: () => Any)

}
