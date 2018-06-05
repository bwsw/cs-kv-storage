package com.bwsw.cloudstack.storage.kv.message

case class KvHistoryFlush(values: Map[KvHistory, Int])
