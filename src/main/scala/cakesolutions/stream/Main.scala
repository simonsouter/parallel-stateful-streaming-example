package cakesolutions.stream

import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory


object Main extends App {
  val log = LoggerFactory.getLogger(Main.getClass.getName)

  val host: String = "192.168.99.100"
  val port: Int = 9042

  val stateRepo = new StateRepo(host, port)
  val config = ConfigFactory.load().getConfig("consumer")

  Stream(config, stateRepo)
}
