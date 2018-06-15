package com.bwsw.cloudstack.storage.kv.mock

import akka.actor.Actor
import akka.actor.Status.Failure
import com.bwsw.cloudstack.storage.kv.mock.MockActor._
import org.scalatest.Matchers

import scala.collection.mutable

class MockActor() extends Actor with Matchers {

  private val queue = mutable.Queue.empty[Expectation]
  private val errors = mutable.ListBuffer.empty[Exception]

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
          errors += e
          sender ! Failure(e)
      }
  }

  def expect(expectation: Expectation): Unit = queue.enqueue(expectation)

  def clearAndExpect(expectation: Expectation): Unit = {
    clear()
    expect(expectation)
  }

  def check(): Unit = {
    errors.foreach(e => throw e)
    queue.foreach(expectation =>
      fail("Unsatisfied expectation " + expectation)
    )
  }

  def clear(): Unit = {
    queue.clear()
    errors.clear()
  }
}

object MockActor {

  sealed trait Expectation {
    val message: Any
  }

  case class ResponsiveExpectation(message: Any, responseProducer: () => Any) extends Expectation

  case class SilentExpectation(message: Any) extends Expectation

}
