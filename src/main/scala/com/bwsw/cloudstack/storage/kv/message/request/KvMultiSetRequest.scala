package com.bwsw.cloudstack.storage.kv.message.request

case class KvMultiSetRequest(storage: String, kvs: Map[String, String]) extends KvRequest
