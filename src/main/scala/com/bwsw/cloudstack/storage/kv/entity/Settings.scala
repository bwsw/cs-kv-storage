package com.bwsw.cloudstack.storage.kv.entity

/** Set of storage settings
  */
case class Settings(
    keySize: Int,
    valueSize: Int,
    valueLimit: Int,
    throttling: Int,
    maxLifetime: Long) {
}
