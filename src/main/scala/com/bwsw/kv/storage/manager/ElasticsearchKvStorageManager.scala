package com.bwsw.kv.storage.manager

import com.bwsw.kv.storage.error.{BadRequestError, InternalError, NotFoundError, StorageError}
import com.bwsw.kv.storage.processor.ElasticsearchKvProcessor.getError
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.delete.DeleteResponse
import com.sksamuel.elastic4s.http.get.GetResponse
import com.sksamuel.elastic4s.http.index.admin.DeleteIndexResponse
import com.sksamuel.elastic4s.http.{HttpClient, RequestFailure, RequestSuccess}

import scala.concurrent.Future

/** A manager for Elasticsearch storages.
  *
  * @param client the client to send requests to Elasticsearch
  */
class ElasticsearchKvStorageManager(client: HttpClient) extends KvStorageManager {

  import ElasticsearchKvStorageManager._
  import scala.concurrent.ExecutionContext.Implicits.global

  def updateTempStorageTtl(storage: String, ttl: Long): Future[Either[StorageError, Unit]] = {
    client.execute(update(storage) in Registry / Type
      script "if (ctx._source.type == \"TEMP\"){ ctx._source.ttl = " + ttl + " } else { ctx.op=\"noop\"}") //TODO: test this
      //      updateByQuery(Registry, Type, boolQuery.must(idsQuery(Seq(storage)), termQuery("type", "TEMP"))))
      .map {
      case Left(failure: RequestFailure) => failure.status match {
        case 404 => Left(NotFoundError())
        case _ => Left(getError(failure))
      }
      case Right(RequestSuccess(status, body, headers, updateRequest)) => updateRequest.result match {
        case "updated" => Right(Unit)
        case "noop" => Left(BadRequestError())
      }
    }
  }

  def deleteTempStorage(storage: String): Future[Either[StorageError, Unit]] = {
    def checkType = {
      client.execute(get(Registry, Type, storage)).map {
        case Left(failure: RequestFailure) => Left(getError(failure))
        case Right(success: RequestSuccess[GetResponse]) =>
          if (success.result.source("type") == Temporary)
            Right()
          else
            Left(BadRequestError())
      }
    }

    def tryDeleteRegistryRecord(either: Either[StorageError, Unit]): Future[Either[StorageError, Unit]] = either match {
      case Left(error: StorageError) => Future(Left(error))
      case Right(_) =>
        client.execute(deleteById(Registry, Type, storage)).map {
          case Left(failure: RequestFailure) => Left(getError(failure))
          case Right(_) => Right(Unit)
        }
    }

    def tryDeleteIndex(either: Either[StorageError, Unit]): Future[Either[StorageError, Unit]] = either match {
      case Left(error: StorageError) => Future(Left(error))
      case Right(_) => client.execute(deleteIndex(getIndex(storage))).map {
        case Left(failure: RequestFailure) =>
          if (failure.status == 404) {
            Right(Unit)
          }
          else {
            Left(getError(failure))
          }
        case Right(_) => Right(Unit)
      }
    }

    for {
      getFuture <- checkType
      deleteByIdFuture <- tryDeleteRegistryRecord(getFuture)
      deleteIndexFuture <- tryDeleteIndex(deleteByIdFuture)
    } yield deleteIndexFuture
  }
}

object ElasticsearchKvStorageManager {
  private val Registry = "storage-registry"
  private val Type = "_doc"
  private val Temporary = "TEMP"

  private def getError(requestFailure: RequestFailure): StorageError = {
    if (requestFailure.status == 404)
      NotFoundError()
    else if (requestFailure.error == null)
      InternalError("Elasticsearch error")
    else InternalError(requestFailure.error.reason)
  }

  private def getIndex(storage: String): String = "storage-" + storage
}
