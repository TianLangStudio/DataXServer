package com.tianlangstudio.data.datax.server

import com.tianlangstudio.data.datax.common.Logging
import com.tianlangstudio.data.datax.main.HttpServerMain
import org.scalatest.FunSuite
import org.scalatest.Matchers._

/**
  *
  * Created by zhuhq on 17-4-13.
  */
class HttpServerTest extends FunSuite with Logging {
  test("start httpserver") {
    HttpServerMain.main(Array("1"))
  }
}
