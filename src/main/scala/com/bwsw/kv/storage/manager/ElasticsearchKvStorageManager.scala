package com.bwsw.kv.storage.manager

import com.bwsw.kv.storage.error.{BadRequestError, InternalError, NotFoundError, StorageError}
import com.bwsw.kv.storage.util.ElasticsearchUtils
import com.sksamuel.elastic4s.http.ElasticDsl._
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
      script s"""if (ctx._source.type == "$TemporaryStorageType"){ ctx._source.ttl = $ttl } else { ctx.op="noop"}""")
      .map {
        case Left(failure) => failure.status match {
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
    def checkType: Future[Either[StorageError, Unit]] = {
      client.execute(get(Registry, Type, storage)).map {
        case Left(failure) => Left(getError(failure))
        case Right(success) =>
          if (success.result.found)
            if (success.result.source("type") == TemporaryStorageType)
              Right()
            else
              Left(BadRequestError())
          else Left(NotFoundError())
      }
    }

    def deleteStorage(either: Either[StorageError, Unit]): Future[Either[StorageError, Unit]] = either match {
      case Left(error) => Future(Left(error))
      case Right(_) =>
        client.execute(deleteById(Registry, Type, storage)).map {
          case Left(failure) => Left(getError(failure))
          case Right(_) => Right(Unit)
        }
    }

    def deleteStorageData(either: Either[StorageError, Unit]): Future[Either[StorageError, Unit]] = either match {
      case Left(_: NotFoundError) => deleteStorageIndex(true)
      case Left(error: StorageError) => Future(Left(error))
      case Right(_) => deleteStorageIndex(false)
    }

    def deleteStorageIndex(storageNotFound: Boolean): Future[Either[StorageError, Unit]] = {
      client.execute(deleteIndex(ElasticsearchUtils.getStorageIndex(storage)))
        .map {
          case Left(failure) =>
            if (failure.status == 404) {
              if (storageNotFound)
                Left(NotFoundError())
              else
                Right(Unit)
            } else {
              Left(getError(failure))
            }
          case Right(_) => Right(Unit)
        }
    }

    for {
      getFuture <- checkType
      deleteByIdFuture <- deleteStorage(getFuture)
      deleteIndexFuture <- deleteStorageData(deleteByIdFuture)
    } yield deleteIndexFuture
  }
}

/** ElasticsearchKvStorageManager companion object. **/
object ElasticsearchKvStorageManager {
  private val Registry = "storage-registry"
  private val Type = "_doc"
  private val TemporaryStorageType = "TEMP"

  private def getError(requestFailure: RequestFailure): StorageError = {
    if (requestFailure.status == 404)
      NotFoundError()
    else if (requestFailure.error == null)
      InternalError("Elasticsearch error")
    else InternalError(requestFailure.error.reason)
  }
}
