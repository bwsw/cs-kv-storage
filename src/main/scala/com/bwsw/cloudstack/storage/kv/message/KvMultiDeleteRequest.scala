package com.bwsw.cloudstack.storage.kv.message

case class KvMultiDeleteRequest(storage: String, keys: Iterable[String])
