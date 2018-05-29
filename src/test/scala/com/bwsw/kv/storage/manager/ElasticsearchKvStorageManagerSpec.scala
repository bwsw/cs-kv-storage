package com.bwsw.kv.storage.manager

import com.bwsw.kv.storage.error.{BadRequestError, InternalError, NotFoundError}
import com.sksamuel.elastic4s.delete.DeleteByIdDefinition
import com.sksamuel.elastic4s.get.GetDefinition
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.delete.DeleteResponse
import com.sksamuel.elastic4s.http.get.GetResponse
import com.sksamuel.elastic4s.http.index.admin.DeleteIndexResponse
import com.sksamuel.elastic4s.http.{update => _, _}
import com.sksamuel.elastic4s.http.update.UpdateResponse
import com.sksamuel.elastic4s.indexes.DeleteIndex
import com.sksamuel.elastic4s.update.UpdateDefinition
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.AsyncFunSpec

import scala.concurrent.{ExecutionContext, Future}

class ElasticsearchKvStorageManagerSpec extends AsyncFunSpec with AsyncMockFactory {

  private val storage = "someStorage"
  private val ttl = 300000
  private val registry = "storage-registry"
  private val `type` = "_doc"
  private val typeField = "type"
  private val temporary = "TEMP"
  private val vm = "VM"


  describe("An ElasticsearchStorageManager") {
    val fakeClient = mock[HttpClient]
    val manager = new ElasticsearchKvStorageManager(fakeClient)


    describe("(update temp storage ttl)") {
      it("should update the value of ttl field in storage registry") {
        val updateResponse = UpdateResponse(registry, `type`, storage, 2, "updated", forcedRefresh = false, Shards(2, 1, 0), None)
        expectUpdateRequest(fakeClient).returning(getRequestSuccessFuture(updateResponse))
        manager.updateTempStorageTtl(storage, ttl).map {
          case Right(_) => succeed
          case _ => fail
        }
      }
      it("should return BadRequestError if type of the storage isn't TEMP") {
        val updateResponse = UpdateResponse(registry, `type`, storage, 2, "noop", forcedRefresh = false, Shards(2, 1, 0), None)
        expectUpdateRequest(fakeClient).returning(getRequestSuccessFuture(updateResponse))
        manager.updateTempStorageTtl(storage, ttl).map {
          case Left(_: BadRequestError) => succeed
          case _ => fail
        }
      }
      it("should return NotFoundError if there is no such storage") {
        expectUpdateRequest(fakeClient).returning(getRequestFailureFuture(404))
        manager.updateTempStorageTtl(storage, ttl).map {
          case Left(_: NotFoundError) => succeed
          case _ => fail
        }
      }
      it("should return InternalError if the request fails") {
        expectUpdateRequest(fakeClient).returning(getRequestFailureFuture(500))
        manager.updateTempStorageTtl(storage, ttl).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }
    }

    describe("(delete temp storage)") {
      it("should delete the storage and associated record in a registry") {
        val getResponse = GetResponse(storage, registry, `type`, 2, found = true, Map.empty, Map(typeField -> temporary))
        val deleteByIdResponse = DeleteResponse(Shards(2, 1, 0), registry, `type`, storage, 2, "deleted")
        val deleteIndexResponse = DeleteIndexResponse(true)
        expectGetRequest(fakeClient).returning(getRequestSuccessFuture(getResponse))
        expectDeleteByIdRequest(fakeClient).returning(getRequestSuccessFuture(deleteByIdResponse))
        expectDeleteIndexRequest(fakeClient).returning(getRequestSuccessFuture(deleteIndexResponse))

        manager.deleteTempStorage(storage).map {
          case Right(_) => succeed
          case _ => fail
        }
      }
      it("should return BadRequestError if type of the storage isn't TEMP") {
        val getResponse = GetResponse(storage, registry, `type`, 2, found = true, Map.empty, Map(typeField -> vm))
        expectGetRequest(fakeClient).returning(getRequestSuccessFuture(getResponse))
        expectDeleteByIdRequest(fakeClient).never
        expectDeleteIndexRequest(fakeClient).never
        manager.deleteTempStorage(storage).map {
          case Left(_: BadRequestError) => succeed
          case _ => fail
        }
      }
      it("should return NotFoundError if there is no such storage in registry (during get)") {
        expectGetRequest(fakeClient).returning(getRequestFailureFuture(404))
        expectDeleteByIdRequest(fakeClient).never
        expectDeleteIndexRequest(fakeClient).never
        manager.deleteTempStorage(storage).map {
          case Left(_: NotFoundError) => succeed
          case _ => fail
        }
      }
      it("should return NotFoundError if there is no such storage in registry (during delete)") {
        val getResponse = GetResponse(storage, registry, `type`, 2, found = true, Map.empty, Map(typeField -> temporary))
        expectGetRequest(fakeClient).returning(getRequestSuccessFuture(getResponse))
        expectDeleteByIdRequest(fakeClient).returning(getRequestFailureFuture(404))
        expectDeleteIndexRequest(fakeClient).never
        manager.deleteTempStorage(storage).map {
          case Left(_: NotFoundError) => succeed
          case _ => fail
        }
      }
      it("should return InternalError if get request fails") {
        expectGetRequest(fakeClient).returning(getRequestFailureFuture(500))
        expectDeleteByIdRequest(fakeClient).never
        expectDeleteIndexRequest(fakeClient).never
        manager.deleteTempStorage(storage).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }
      it("should return InternalError if delete by id request fails") {
        val getResponse = GetResponse(storage, registry, `type`, 2, found = true, Map.empty, Map(typeField -> temporary))
        expectGetRequest(fakeClient).returning(getRequestSuccessFuture(getResponse))
        expectDeleteByIdRequest(fakeClient).returning(getRequestFailureFuture(500))
        expectDeleteIndexRequest(fakeClient).never
        manager.deleteTempStorage(storage).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }
      it("should return InternalError if delete index request fails") {
        val getResponse = GetResponse(storage, registry, `type`, 2, found = true, Map.empty, Map(typeField -> temporary))
        val deleteByIdResponse = DeleteResponse(Shards(2, 1, 0), registry, `type`, storage, 2, "deleted")
        expectGetRequest(fakeClient).returning(getRequestSuccessFuture(getResponse))
        expectDeleteByIdRequest(fakeClient).returning(getRequestSuccessFuture(deleteByIdResponse))
        expectDeleteIndexRequest(fakeClient).returning(getRequestFailureFuture(500))
        manager.deleteTempStorage(storage).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }
      it("should return Right(Unit) if delete index request returns 404") {
        val getResponse = GetResponse(storage, registry, `type`, 2, found = true, Map.empty, Map(typeField -> temporary))
        val deleteByIdResponse = DeleteResponse(Shards(2, 1, 0), registry, `type`, storage, 2, "deleted")
        expectGetRequest(fakeClient).returning(getRequestSuccessFuture(getResponse))
        expectDeleteByIdRequest(fakeClient).returning(getRequestSuccessFuture(deleteByIdResponse))
        expectDeleteIndexRequest(fakeClient).returning(getRequestFailureFuture(404))
        manager.deleteTempStorage(storage).map {
          case Right(_) => succeed
          case _ => fail
        }
      }
    }
  }

  private def expectUpdateRequest(client: HttpClient) = {
    (client.execute[UpdateDefinition, UpdateResponse](_: UpdateDefinition)(_: HttpExecutable[UpdateDefinition, UpdateResponse], _: ExecutionContext))
      .expects(update(storage) in registry / `type`
        script "if (ctx._source.type == \"TEMP\"){ ctx._source.ttl = " + ttl + " } else { ctx.op=\"noop\"}", UpdateHttpExecutable, *)
  }

  private def expectGetRequest(client: HttpClient) = {
    (client.execute[GetDefinition, GetResponse](_: GetDefinition)(_: HttpExecutable[GetDefinition, GetResponse], _: ExecutionContext))
      .expects(ElasticDsl.get(storage).from(registry / `type`), GetHttpExecutable, *)
  }

  private def expectDeleteByIdRequest(client: HttpClient) = {
    (client.execute[DeleteByIdDefinition, DeleteResponse](_: DeleteByIdDefinition)(_: HttpExecutable[DeleteByIdDefinition, DeleteResponse], _: ExecutionContext))
      .expects(deleteById(registry, `type`, storage), DeleteByIdExecutable, *)
  }

  private def expectDeleteIndexRequest(client: HttpClient) = {
    (client.execute[DeleteIndex, DeleteIndexResponse](_: DeleteIndex)(_: HttpExecutable[DeleteIndex, DeleteIndexResponse], _: ExecutionContext))
      .expects(deleteIndex(getIndex(storage)), DeleteIndexHttpExecutable, *)
  }

  private def getRequestSuccessFuture[T](response: T): Future[Right[RequestFailure, RequestSuccess[T]]] = {
    Future(Right(RequestSuccess(200, Option.empty, Map.empty, response)))
  }

  private def getRequestFailureFuture[T](status: Int): Future[Left[RequestFailure, RequestSuccess[T]]] = {
    Future(Left(RequestFailure(status, Option.empty, Map.empty, ElasticError.fromThrowable(new RuntimeException()))))
  }

  private def getIndex(storage: String): String = "storage-" + storage
}
