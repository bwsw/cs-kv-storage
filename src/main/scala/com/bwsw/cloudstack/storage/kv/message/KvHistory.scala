package com.bwsw.cloudstack.storage.kv.message

case class KvHistory(storage: String, key: String, value: String, timestamp: Long, operation: Operation, attempt: Int = 0) {
  /** Returns copy of this history with incremented attempts
    *
    * @return new KvHistory
    */
  def makeAttempt: KvHistory = {
    KvHistory(storage, key, value, timestamp, operation, attempt + 1)
  }
}
