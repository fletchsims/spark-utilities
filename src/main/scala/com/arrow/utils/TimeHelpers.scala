package com.arrow.utils

import java.time.LocalDate

object TimeHelpers {
  val TODAY: LocalDate = LocalDate.now()
  val TODAY_STRING: String = LocalDate.now().toString
}
