package com.bwsw.cloudstack.storage.kv.message

import com.bwsw.cloudstack.storage.kv.error.StorageError

case class KvListResponse(response: Either[StorageError, List[String]])
