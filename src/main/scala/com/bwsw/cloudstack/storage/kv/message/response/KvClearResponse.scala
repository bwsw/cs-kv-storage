package com.bwsw.cloudstack.storage.kv.message.response

import com.bwsw.cloudstack.storage.kv.entity.Storage
import com.bwsw.cloudstack.storage.kv.error.StorageError

case class KvClearResponse(storage: Storage, timestamp: Long, response: Either[StorageError, Unit]) extends KvResponse
