package com.tomshley.boilerplate.jvm.utils

import com.github.nscala_time.time.{Implicits, StaticDateTime, StaticDateTimeFormat, StaticDateTimeZone, StaticDuration, StaticInterval, StaticLocalDate, StaticLocalDateTime, StaticLocalTime, StaticMonthDay, StaticPartial, StaticPeriod, StaticYearMonth}

// Deprecated: java.time (JSR-310), designed by the Joda-Time author, ships with
// the JDK since Java 8 and is the standard replacement. The type aliases and
// forwarders here no longer add value over direct java.time imports.
@deprecated("Use java.time directly", "next")
object TimeUtils extends TimeUtils

@deprecated("Use java.time directly", "next")
object TimeUtilsTypes extends TimeUtilsTypes

@deprecated("Use java.time directly", "next")
object TimeUtilsForwarder extends TimeUtilsForwarder

@deprecated("Use java.time directly", "next")
trait TimeUtils extends TimeUtilsTypes with TimeUtilsForwarder with Implicits

@deprecated("Use java.time directly", "next")
trait TimeUtilsTypes {
  type Chronology = org.joda.time.Chronology
  type DateTime = org.joda.time.DateTime
  type DateTimeFormat = org.joda.time.format.DateTimeFormat
  type DateTimeZone = org.joda.time.DateTimeZone
  type Duration = org.joda.time.Duration
  type Interval = org.joda.time.Interval
  type LocalDate = org.joda.time.LocalDate
  type LocalDateTime = org.joda.time.LocalDateTime
  type LocalTime = org.joda.time.LocalTime
  type Period = org.joda.time.Period
  type Partial = org.joda.time.Partial
  type YearMonth = org.joda.time.YearMonth
  type MonthDay = org.joda.time.MonthDay
}

@deprecated("Use java.time directly", "next")
trait TimeUtilsForwarder {
  val DateTime: StaticDateTime.type = com.github.nscala_time.time.StaticDateTime
  val DateTimeFormat: StaticDateTimeFormat.type = com.github.nscala_time.time.StaticDateTimeFormat
  val DateTimeZone: StaticDateTimeZone.type = com.github.nscala_time.time.StaticDateTimeZone
  val Duration: StaticDuration.type = com.github.nscala_time.time.StaticDuration
  val Interval: StaticInterval.type = com.github.nscala_time.time.StaticInterval
  val LocalDate: StaticLocalDate.type = com.github.nscala_time.time.StaticLocalDate
  val LocalDateTime: StaticLocalDateTime.type = com.github.nscala_time.time.StaticLocalDateTime
  val LocalTime: StaticLocalTime.type = com.github.nscala_time.time.StaticLocalTime
  val Period: StaticPeriod.type = com.github.nscala_time.time.StaticPeriod
  val Partial: StaticPartial.type = com.github.nscala_time.time.StaticPartial
  val YearMonth: StaticYearMonth.type = com.github.nscala_time.time.StaticYearMonth
  val MonthDay: StaticMonthDay.type = com.github.nscala_time.time.StaticMonthDay
}
