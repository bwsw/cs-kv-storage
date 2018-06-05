package com.bwsw.cloudstack.storage.kv.message.request

case class KvMultiGetRequest(storage: String, keys: Iterable[String])
