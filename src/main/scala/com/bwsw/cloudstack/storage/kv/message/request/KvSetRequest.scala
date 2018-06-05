package com.bwsw.cloudstack.storage.kv.message.request

case class KvSetRequest(storage: String, key: String, value: String)
