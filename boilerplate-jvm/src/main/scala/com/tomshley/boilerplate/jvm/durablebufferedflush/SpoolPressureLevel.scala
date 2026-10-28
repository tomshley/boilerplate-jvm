/*
 * copyright 2026 tomshley llc
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

package com.tomshley.boilerplate.jvm.durablebufferedflush

/** Coarse-grained classification of spool occupancy with three levels and
  * hysteresis between them. Owned and emitted by [[SpoolPressureMonitor]]
  * on its periodic tick. Subscribers (typically an [[AdmissionController]])
  * react to level transitions.
  *
  * The levels intentionally lose detail compared to a raw byte count: this
  * is the contract between measurement and policy. A subscriber that wants
  * to react to "we are 92% full" instead of "Critical" is misusing the
  * abstraction — that intent belongs in [[SpoolPressureConfig]] thresholds,
  * not in the subscriber. */
enum SpoolPressureLevel:

  /** Below the alert threshold — normal operation. */
  case Low

  /** At or above the alert threshold, below the critical threshold. The
    * canonical signal for paging operators: there is still admission
    * headroom, but capacity should be reviewed. */
  case High

  /** At or above the critical threshold. The canonical signal to close
    * admission for new sessions; in-flight sessions remain unaffected
    * (they have already paid the cost of being admitted and would be
    * harder to recover from a hard failure than to let drain). */
  case Critical
