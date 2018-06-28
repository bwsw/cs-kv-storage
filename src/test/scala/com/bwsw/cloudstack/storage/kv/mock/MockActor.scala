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
