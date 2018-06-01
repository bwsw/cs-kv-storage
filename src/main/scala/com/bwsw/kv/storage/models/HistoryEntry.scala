package com.bwsw.kv.storage.models

case class HistoryEntry(storageUuid: String, key: String, value: String, timestamp: Long)
