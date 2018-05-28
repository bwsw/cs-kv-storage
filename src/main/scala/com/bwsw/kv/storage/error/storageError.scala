package com.bwsw.kv.storage.error

sealed trait StorageError

case class InternalError(message: String) extends StorageError

case class NotFoundError() extends StorageError

case class ConflictError() extends StorageError

case class BadRequestError() extends StorageError
