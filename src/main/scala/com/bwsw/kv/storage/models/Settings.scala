package com.bwsw.kv.storage.models

/** Set of storage settings
  */
case class Settings(
    keySize: Int,
    valueSize: Int,
    valueLimit: Int,
    throttling: Int,
    maxLifetime: Long) {
}
