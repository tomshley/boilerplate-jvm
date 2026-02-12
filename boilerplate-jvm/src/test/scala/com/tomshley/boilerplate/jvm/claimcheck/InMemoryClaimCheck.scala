package com.tomshley.boilerplate.jvm.claimcheck

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

final class InMemoryClaimCheck(
    defaultLocation: String
)(using ec: ExecutionContext) extends ClaimCheck {

  private val storeType: String = "in-memory"
  private val dataByKey: TrieMap[(String, String), Array[Byte]] = TrieMap.empty

  override def check(data: Array[Byte], tag: String): Future[ClaimTicket] = {
    dataByKey.put((defaultLocation, tag), data)
    Future.successful(ClaimTicket(number = tag, location = defaultLocation, storeType = storeType, sizeBytes = Some(data.length.toLong)))
  }

  override def claim(ticket: ClaimTicket): Future[Option[Array[Byte]]] =
    Future.successful(dataByKey.get((ticket.location, ticket.number)))

  override def discard(ticket: ClaimTicket): Future[Boolean] =
    Future.successful(dataByKey.remove((ticket.location, ticket.number)).isDefined)

  override def exists(ticket: ClaimTicket): Future[Boolean] =
    Future.successful(dataByKey.contains((ticket.location, ticket.number)))
}
