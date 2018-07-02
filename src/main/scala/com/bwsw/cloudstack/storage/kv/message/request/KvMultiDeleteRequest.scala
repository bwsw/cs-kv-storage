package com.bwsw.cloudstack.storage.kv.message.request

case class KvMultiDeleteRequest(storage: String, keys: Iterable[String]) extends KvRequest
