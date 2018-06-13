package com.bwsw.cloudstack.storage.kv.message.response

import com.bwsw.cloudstack.storage.kv.error.StorageError

case class KvErrorResponse(error: StorageError) extends KvResponse
