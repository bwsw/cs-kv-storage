package com.bwsw.cloudstack.storage.kv.message.request

case class KvGetRequest(storage: String, key: String) extends KvRequest
