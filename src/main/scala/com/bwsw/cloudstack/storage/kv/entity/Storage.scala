package com.bwsw.cloudstack.storage.kv.entity

/** Abstract Key-Value Storage with basic methods
  */
abstract class Storage {
  val uUID: String
  val keepHistory: Boolean
}
