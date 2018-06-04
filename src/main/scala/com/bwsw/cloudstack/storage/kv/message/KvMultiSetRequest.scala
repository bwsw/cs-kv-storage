package com.bwsw.cloudstack.storage.kv.message

case class KvMultiSetRequest(storage: String, kvs: Map[String, String])
