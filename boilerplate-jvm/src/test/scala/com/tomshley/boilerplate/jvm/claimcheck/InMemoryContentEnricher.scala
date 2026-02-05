package com.tomshley.boilerplate.jvm.claimcheck

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

final class InMemoryContentEnricher(using ec: ExecutionContext) extends ContentEnricher {
  override val storeType: String = "in-memory"

  private val dataByKey: TrieMap[(String, String), Array[Byte]] = TrieMap.empty

  override def store(
      key: String,
      data: Array[Byte],
      location: String
  ): Future[ClaimTicket] = {
    dataByKey.put((location, key), data)
    Future.successful(SimpleClaimTicket(number = key, location = location, storeType = storeType, sizeBytes = Some(data.length.toLong)))
  }

  override def retrieve(ticket: ClaimTicket): Future[Option[Array[Byte]]] =
    Future.successful(dataByKey.get((ticket.location, ticket.number)))

  override def delete(ticket: ClaimTicket): Future[Boolean] =
    Future.successful(dataByKey.remove((ticket.location, ticket.number)).isDefined)

  override def exists(ticket: ClaimTicket): Future[Boolean] =
    Future.successful(dataByKey.contains((ticket.location, ticket.number)))
}
