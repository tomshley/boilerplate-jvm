package com.tomshley.boilerplate.jvm.reqreply.forms

import scala.util.matching.Regex

trait Validations {
  private final val emailExpression: Regex = "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$".r
  private final val phoneExpression: Regex = "^(\\+\\d{1,2}\\s?)?\\(?\\d{3}\\)?[\\s.-]?\\d{3}[\\s.-]?\\d{4}$".r

  def isValidEmailFormat(email: String): Boolean = {

    emailExpression.findFirstMatchIn(email) match {
      case Some(_) => true
      case None => false
    }
  }

  def isValidPhoneFormat(phone: String): Boolean = {
    phoneExpression.findFirstMatchIn(phone) match {
      case Some(_) => true
      case None => false
    }
  }

  def isShortEnough(message: String, limit: Int): Boolean = {
    message.length <= limit
  }
}
