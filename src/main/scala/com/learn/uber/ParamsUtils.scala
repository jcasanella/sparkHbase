package com.learn

import com.learn.uber.StringParamException

import scala.util.Try

object ParamsUtils {

  // Check atrributes
  def validateAttribute(par: String): Try[String] = Try(checkAttribute(par))

  private def checkAttribute(param: String): String = {

    if (param != null && param.length > 0) {
      param
    } else {
      throw new StringParamException("Parameter not valid")
    }
  }
}
