package com.bwsw.cloudstack.storage.kv.message.response

import com.bwsw.cloudstack.storage.kv.error.StorageError

case class KvMultiGetResponse(response: Either[StorageError, Map[String, Option[String]]])
