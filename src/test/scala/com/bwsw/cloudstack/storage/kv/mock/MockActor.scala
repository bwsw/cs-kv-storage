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
import akka.actor.Status.Failure
import com.bwsw.cloudstack.storage.kv.mock.MockActor._
import org.scalatest.Matchers

import scala.collection.mutable

class MockActor() extends Actor with Matchers {

  private val queue = mutable.Queue.empty[Expectation]

  override def receive: Receive = {
    case message: Any =>
      try {
        if (queue.isEmpty) {
          fail("Unexpected message " + message)
        }
        queue.dequeue() match {
          case expectation: ResponsiveExpectation =>
            message should equal(expectation.message)
            sender ! expectation.responseProducer.apply()
          case expectation =>
            message should equal(expectation.message)
        }
      } catch {
        case e: Exception =>
          sender ! Failure(e)
      }
  }

  def expect(expectation: Expectation): Unit = queue.enqueue(expectation)

  def clearAndExpect(expectation: Expectation): Unit = {
    clear()
    expect(expectation)
  }

  def check(): Unit = {
    if (queue.nonEmpty) {
      fail("Unsatisfied expectations:\n" + queue.mkString("\n"))
    }
  }

  def clear(): Unit = {
    queue.clear()
  }
}

object MockActor {

  sealed trait Expectation {
    val message: Any
  }

  case class ResponsiveExpectation(message: Any, responseProducer: () => Any) extends Expectation

  case class SilentExpectation(message: Any) extends Expectation

}
