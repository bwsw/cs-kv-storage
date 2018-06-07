package com.bwsw.cloudstack.storage.kv.cache

import com.bwsw.cloudstack.storage.kv.entity.Storage
import com.sksamuel.elastic4s.http.ElasticDsl.get
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ElasticsearchStorageLoader(client: HttpClient) extends StorageLoader {
  private val registry = "storage-registry"
  private val `type` = "_doc"

  def load: String => Future[Option[Storage]] = {
    id: String =>
      client.execute(get(registry, `type`, id)).map {
        case Left(_) => throw new RuntimeException("Storage info loading failed")
        case Right(success) =>
          if (success.result.found) {
            Some(Storage(success.result.id, getValue(success.result.source, "type"), getValue(success.result.source, "is_history_enabled").toBoolean))
          }
          else None
      }
  }

  private def getValue(source: Map[String, Any], key: String) = {
    source.get(key) match {
      case Some(s: String) => s
      case _ => throw new RuntimeException("Invalid result")
    }
  }
}
