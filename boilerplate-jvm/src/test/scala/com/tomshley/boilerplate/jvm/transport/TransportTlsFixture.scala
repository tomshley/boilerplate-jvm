/*
 * copyright 2017–present tomshley llc
 *
 * licensed under the apache license, version 2.0 (the "license");
 * you may not use this file except in compliance with the license.
 * you may obtain a copy of the license at
 *
 * http://www.apache.org/licenses/license-2.0
 *
 * unless required by applicable law or agreed to in writing, software
 * distributed under the license is distributed on an "as is" basis,
 * without warranties or conditions of any kind, either express or implied.
 * see the license for the specific language governing permissions and
 * limitations under the license.
 *
 * @author thomas schena @sgoggles <https://github.com/sgoggles> | <https://gitlab.com/sgoggles>
 *
 */

package com.tomshley.boilerplate.jvm.transport

import java.nio.file.{Files, Path}
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManager, KeyManagerFactory, SSLContext, SSLEngine, TrustManagerFactory}
import scala.sys.process.{Process, ProcessLogger}
import scala.util.Using

private[transport] object TransportTlsFixture {

  private val StorePassword = "fixture-store-password".toCharArray

  def withContexts[A](test: (SSLContext, SSLContext) => A): A =
    withKeyStore { keyStore =>
      test(
        context(keyStore, keyManagers = true),
        context(keyStore, keyManagers = false)
      )
    }

  def withMutualContexts[A](test: (SSLContext, SSLContext, SSLContext) => A): A =
    withKeyStore { keyStore =>
      test(
        context(keyStore, keyManagers = true),
        context(keyStore, keyManagers = false),
        context(keyStore, keyManagers = true)
      )
    }

  def serverEngine(
      context: SSLContext,
      needClientAuth: Boolean = false
  ): SSLEngine = {
    val engine = context.createSSLEngine()
    engine.setUseClientMode(false)
    engine.setNeedClientAuth(needClientAuth)
    engine
  }

  private def withKeyStore[A](test: KeyStore => A): A = {
    val dir = Files.createTempDirectory("transport-tls-fixture")
    val keyStorePath = dir.resolve("fixture.p12")
    try {
      generateKeyStore(keyStorePath)
      val keyStore = KeyStore.getInstance("PKCS12")
      Using.resource(Files.newInputStream(keyStorePath)) { input =>
        keyStore.load(input, StorePassword)
      }
      test(keyStore)
    } finally {
      Files.deleteIfExists(keyStorePath)
      Files.deleteIfExists(dir)
    }
  }

  private def generateKeyStore(keyStorePath: Path): Unit = {
    val keytool = Path
      .of(System.getProperty("java.home"), "bin", "keytool")
      .toAbsolutePath
      .toString
    val errors = new StringBuilder
    val exit = Process(
      Seq(
        keytool,
        "-genkeypair",
        "-alias", "server",
        "-keyalg", "RSA",
        "-keysize", "2048",
        "-storetype", "PKCS12",
        "-keystore", keyStorePath.toString,
        "-storepass", new String(StorePassword),
        "-keypass", new String(StorePassword),
        "-dname", "CN=loopback.test",
        "-ext", "SAN=ip:127.0.0.1",
        "-validity", "2",
        "-noprompt"
      )
    ).!(ProcessLogger(_ => (), line => errors.append(line).append('\n')))
    require(exit == 0, s"keytool exited with $exit: ${errors.toString.trim}")
  }

  private def context(keyStore: KeyStore, keyManagers: Boolean): SSLContext = {
    val keyManagerArray =
      if (keyManagers) {
        val factory =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
        factory.init(keyStore, StorePassword)
        factory.getKeyManagers
      } else Array.empty[KeyManager]
    val trustFactory =
      TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trustFactory.init(keyStore)
    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(
      keyManagerArray,
      trustFactory.getTrustManagers,
      new SecureRandom()
    )
    sslContext
  }
}
