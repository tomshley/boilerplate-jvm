package com.tomshley.boilerplate.jvm.transport

import org.apache.pekko.http.scaladsl.{HttpsConnectionContext, model}

import scala.concurrent.Future

final case class HttpServerSetup(
    service: model.HttpRequest => Future[model.HttpResponse],
    httpsContext: Option[HttpsConnectionContext] = None
)
