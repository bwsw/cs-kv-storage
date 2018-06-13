package com.bwsw.cloudstack.storage.kv.message.response

import com.bwsw.cloudstack.storage.kv.error.StorageError

case class KvListResponse(response: Either[StorageError, List[String]]) extends KvResponse
