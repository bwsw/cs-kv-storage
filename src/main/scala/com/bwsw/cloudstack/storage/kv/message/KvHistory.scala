package com.bwsw.cloudstack.storage.kv.message

case class KvHistory(storage: String, key: String, value: String, timestamp: Long)
