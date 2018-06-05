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
