package com.bwsw.kv.storage.models.storage

import java.util.UUID

/** Abstract Key-Value Storage with basic methods
  */
abstract class Storage {
  val uUID: String
  //TODO:
  //methods: get, set, list, delete, clear
}