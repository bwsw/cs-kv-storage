package com.bwsw.cloudstack.storage.kv.util

import com.bwsw.cloudstack.storage.kv.configuration.ElasticsearchConfig
import com.bwsw.cloudstack.storage.kv.error.{InternalError, StorageError}
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.ElasticDsl._

import scala.concurrent.Future

class HealthChecker(client: HttpClient, conf: ElasticsearchConfig) {
  private val registryIndex = "storage-registry"

  import scala.concurrent.ExecutionContext.Implicits.global

  def check: Future[Either[StorageError, Boolean]] = {
    val response = for {
      registry <- client.execute(indexExists(registryIndex))
      storageTemplate <- client.execute(getIndexTemplate("storage"))
      historyTemplate <- client.execute(getIndexTemplate("history-storage"))
    } yield (registry, storageTemplate, historyTemplate)
    response.map {
      case (Right(registry), Right(storageTemplate), Right(historyTemplate)) =>
        Right(registry.result.exists && storageTemplate.result.templates.nonEmpty && historyTemplate.result.templates.nonEmpty)
      case _ => Left(InternalError("Elasticsearch error"))
    }

  }
}
