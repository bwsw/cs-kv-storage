package com.bwsw.cloudstack.storage.kv.message.response

import com.bwsw.cloudstack.storage.kv.error.StorageError

case class KvSetResponse(storage: String, key: String, value: String, timestamp: Long, response: Either[StorageError, Unit]) extends KvResponse
