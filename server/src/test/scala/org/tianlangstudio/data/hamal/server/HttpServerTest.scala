package org.tianlangstudio.data.hamal.server

import com.tianlangstudio.data.datax.main.HttpServerMain
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.tianlangstudio.data.hamal.common.Logging

/**
  *
  * Created by zhuhq on 17-4-13.
  */
class HttpServerTest extends FunSuite with Logging {
  test("start httpserver") {
    HttpServerMain.main(Array("1"))
  }
}
