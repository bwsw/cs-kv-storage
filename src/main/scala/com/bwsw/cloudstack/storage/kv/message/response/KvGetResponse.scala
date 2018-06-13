package com.bwsw.cloudstack.storage.kv.message.response

import com.bwsw.cloudstack.storage.kv.error.StorageError

case class KvGetResponse(response: Either[StorageError, String]) extends KvResponse
