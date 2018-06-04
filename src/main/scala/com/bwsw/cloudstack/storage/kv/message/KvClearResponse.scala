package com.bwsw.cloudstack.storage.kv.message

import com.bwsw.cloudstack.storage.kv.error.StorageError

case class KvClearResponse(response: Either[StorageError, Unit])
