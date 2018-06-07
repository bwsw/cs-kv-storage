package com.bwsw.cloudstack.storage.kv.message

sealed trait Operation

object Set extends Operation {
  override def toString: String = "set"
}

object Delete extends Operation {
  override def toString: String = "delete"
}

object Clear extends Operation {
  override def toString: String = "clear"
}
