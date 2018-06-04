package com.bwsw.cloudstack.storage.kv.message

case class KvMultiGetRequest(storage: String, keys: Iterable[String])
