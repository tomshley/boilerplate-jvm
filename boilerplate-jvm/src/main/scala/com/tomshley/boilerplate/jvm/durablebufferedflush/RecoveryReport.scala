/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

final case class RecoveryReport(
    sessionsRecovered: Int,
    sessionsAborted: Int,
    sessionsCleaned: Int,
    sessionsFailed: Int,
    totalClaimsResent: Long
) {
  def total: Int = sessionsRecovered + sessionsAborted + sessionsCleaned + sessionsFailed
}

object RecoveryReport {
  val empty: RecoveryReport = RecoveryReport(0, 0, 0, 0, 0L)
}
