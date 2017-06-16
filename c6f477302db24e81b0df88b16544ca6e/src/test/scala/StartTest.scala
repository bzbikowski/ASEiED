import org.scalatest.{FunSpec, GivenWhenThen}

class StartTest extends FunSpec with GivenWhenThen {

  describe("StartTest") {
    val app = new Start
    app.getData
  }

}
