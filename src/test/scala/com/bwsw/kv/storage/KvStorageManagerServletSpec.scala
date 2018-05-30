package com.bwsw.kv.storage

import akka.actor.ActorSystem
import com.bwsw.kv.storage.error.{BadRequestError, NotFoundError, StorageError, InternalError}
import com.bwsw.kv.storage.manager.KvStorageManager
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSpecLike
import org.scalatra.test.scalatest.ScalatraSuite

import scala.concurrent.Future

class KvStorageManagerServletSpec
  extends ScalatraSuite
    with FunSpecLike
    with MockFactory {

  import scala.concurrent.ExecutionContext.Implicits.global

  private val system = ActorSystem()
  private val manager = mock[KvStorageManager]
  private val storage = "somestorage"
  private val path = "/storage/" + storage
  private val ttl = 300000
  private val ttlParams = Seq(("ttl", ttl.toString))
  private val badTtlParams = Seq(("ttl", "badTTL"))
  private val message = "test error"

  describe("a KvStorageManagerServlet") {
    addServlet(new KvStorageManagerServlet(system, manager), "/storage/*")

    describe("(update temp storage ttl)") {
      def testProvidesError(error: StorageError, status: Int) = {
        (manager.updateTempStorageTtl(_: String, _: Long)).expects(storage, ttl).returning(Future(Left(error))).once

        put(path, ttlParams) {
          status should equal(status)
        }
      }

      it("should update the value of ttl field in storage registry") {
        (manager.updateTempStorageTtl(_: String, _: Long)).expects(storage, ttl).returning(Future(Right())).once
        put(path, ttlParams) {
          status should equal(200)
        }
      }

      it("should return 400 BadRequest if no ttl specified") {
        (manager.updateTempStorageTtl(_: String, _: Long)).expects(storage, ttl).never
        put(path) {
          status should equal(400)
        }
      }
      it("should return 400 BadRequest if ttl could not be converted to Long") {
        (manager.updateTempStorageTtl(_: String, _: Long)).expects(storage, ttl).never
        put(path, badTtlParams) {
          status should equal(400)
        }
      }
      it("should return 400 BadRequest if manager returned BadRequestError") {
        testProvidesError(BadRequestError(), 400)
      }
      it("should return 404 NotFound if manager returned NotFoundError") {
        testProvidesError(NotFoundError(), 404)
      }
      it("should return 500 InternalServerError if manager returned InternalError") {
        testProvidesError(InternalError(message), 500)
      }
    }
  }

  describe("(delete temp storage)") {
    def testProvidesError(error: StorageError, status: Int) = {
      (manager.deleteTempStorage(_: String)).expects(storage).returning(Future(Left(error))).once

      delete(path) {
        status should equal(status)
      }
    }

    it("should update the value of ttl field in storage registry") {
      (manager.deleteTempStorage(_: String)).expects(storage).returning(Future(Right())).once
      delete(path) {
        status should equal(200)
      }
    }
    it("should return 400 BadRequest if manager returned BadRequestError") {
      testProvidesError(BadRequestError(), 400)
    }
    it("should return 404 NotFound if manager returned NotFoundError") {
      testProvidesError(NotFoundError(), 404)
    }
    it("should return 500 InternalServerError if manager returned InternalError") {
      testProvidesError(InternalError(message), 500)
    }
  }
}
