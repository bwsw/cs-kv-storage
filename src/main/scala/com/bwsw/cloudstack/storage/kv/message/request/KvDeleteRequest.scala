package com.bwsw.cloudstack.storage.kv.message.request

case class KvDeleteRequest(storage: String, key: String) extends KvRequest
