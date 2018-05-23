package com.bwsw.kv.storage.error

import com.sksamuel.elastic4s.http.RequestFailure

sealed trait StorageError extends Exception

case class InternalError(requestFailure: RequestFailure) extends StorageError

case class NotFoundError() extends StorageError