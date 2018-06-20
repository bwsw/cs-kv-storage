package com.bwsw.cloudstack.storage.kv.message.response

import com.bwsw.cloudstack.storage.kv.entity.History

case class GetHistoryResponse(total: Long, size: Long, page: Long, scrollId: Option[String], items: List[History])
