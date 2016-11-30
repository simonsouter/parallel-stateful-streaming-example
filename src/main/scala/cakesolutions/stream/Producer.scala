package cakesolutions.stream

import cakesolutions.kafka.KafkaProducer
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.slf4j.LoggerFactory

object Producer extends App {
  val log = LoggerFactory.getLogger(Main.getClass.getName)

  val config = ConfigFactory.load().getConfig("producer")

  val producerConfig: KafkaProducer.Conf[Integer, Integer] = {
    KafkaProducer.Conf(new IntegerSerializer(),
      new IntegerSerializer()).withConf(config)
  }

  val producer = KafkaProducer(producerConfig)
  1 to 1000 foreach { x =>
    1 to 1000 foreach { y =>
      send(x, y)
    }
  }
//  send(8,10)
  producer.flush()

  def send(key: Int, value: Int) = {
    producer.send(new ProducerRecord("t3", key, value))
  }
}


