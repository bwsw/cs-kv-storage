package com.bwsw.cloudstack.storage.kv.entity

import com.bwsw.cloudstack.storage.kv.message.Operation

case class History(key: String, value: String, timestamp: Long, operation: Operation)
