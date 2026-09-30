package com.tomshley.boilerplate.jvm.transport

import javax.net.ssl.SSLEngine

final case class TcpServerSetup(
    handler: TcpServerHandlerBoilerplate,
    createSSLEngine: Option[() => SSLEngine] = None
)
