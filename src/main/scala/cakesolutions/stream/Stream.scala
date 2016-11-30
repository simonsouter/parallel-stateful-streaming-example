package cakesolutions.stream

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import cakesolutions.kafka.{KafkaConsumer, KafkaTopicPartition}
import cakesolutions.kafka.akka.KafkaConsumerActor.{Confirm, Subscribe}
import cakesolutions.kafka.akka.{ConsumerRecords, KafkaConsumerActor, Offsets}
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.IntegerDeserializer

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object Stream {

  private case class PartitionsAssigned(assignments: Map[TopicPartition, OffsetAndState])

  private case class PartitionsRevoked(revocations: List[TopicPartition])

  def apply(config: Config, stateRepo: StateRepo): ActorRef = {
    val consumerConf = KafkaConsumer.Conf(
      new IntegerDeserializer,
      new IntegerDeserializer,
      groupId = "test_group",
      enableAutoCommit = false,
      autoOffsetReset = OffsetResetStrategy.EARLIEST)
      .withConf(config)

    val actorConf = KafkaConsumerActor.Conf(1.seconds, 3.seconds)

    val system = ActorSystem()
    system.actorOf(Props(new Stream(consumerConf, actorConf, stateRepo)))
  }
}

class Stream(
  kafkaConfig: KafkaConsumer.Conf[Integer, Integer],
  actorConfig: KafkaConsumerActor.Conf,
  stateRepo: StateRepo) extends Actor with ActorLogging {

  import Stream._

  // Cache of Offset and State for each Kafka Partition
  private var cache: Map[TopicPartition, OffsetAndState] = Map.empty

  private val recordsExt = ConsumerRecords.extractor[Integer, Integer]

  private val consumer = context.actorOf(
    KafkaConsumerActor.props(kafkaConfig, actorConfig, self)
  )

  consumer ! Subscribe.AutoPartitionWithManualOffset(List("t3"), assignedListener, revokedListener)

  override def receive: Receive = {

    // Records from Kafka
    case recordsExt(records) =>
      log.info(s"Received Batch of ${records.size} records")
      processRecords(records)

    case PartitionsAssigned(assignments) =>
      log.info("Assignments!!")
      cache = cache ++ assignments

    case PartitionsRevoked(revocations) =>
      log.info("Revocations!!")
      cache = cache -- revocations
  }

  private def processRecords(records: ConsumerRecords[Integer, Integer]) = {

    //Update state
    val updated = records.recordsList.flatMap { record =>
      log.debug(s"Received: key: ${record.key()}, value: ${record.value()}")
      val meterId = record.key().intValue()
      val reading = record.value().intValue()

      // Get state for this partition
      val offsetAndState = cache.get(KafkaTopicPartition(record.topic(), record.partition()))

      offsetAndState.map { offsetAndState =>
        val state = offsetAndState.state.getOrElse(meterId, MeterState(0, 0))
        val newState = state.update(reading)

        val newOffsetAndState = offsetAndState.copy(offset = record.offset() + 1, state = offsetAndState.state + (meterId -> newState))
        cache = cache + (KafkaTopicPartition(record.topic(), record.partition()) -> newOffsetAndState)

        TotalUsage(record.topic(), record.partition(), meterId, newOffsetAndState.offset, newState.samples, newState.total)
      }

      //TODO what if no tp in cache?
    }

    val res = updated.map { update =>
      stateRepo.save(update)
    }
    val x = Future.sequence(res)
    x.onComplete { (rs) =>
      consumer ! Confirm(records.offsets, commit = true)
    }
  }

  private def assignedListener(tps: List[TopicPartition]): Offsets = {
    log.info("Partitions have been assigned" + tps.toString())

    val res = tps.map { tp =>

      val dbRes = stateRepo.findOffsetByKey(tp.topic(), tp.partition())

      //Send the loaded state for the new assignments to ourselves.
      dbRes.foreach { assignments =>
        self ! PartitionsAssigned(Map(KafkaTopicPartition(tp.topic(), tp.partition()) -> assignments))
      }

      // Return the latest offsets for the assigned partitions ot Kafka.
      dbRes.map { offsetAndState => tp -> offsetAndState.offset }
    }

    // We block the Kafka assignment listener thread as we need to provide it a result
    val offsetMap = Await.result(Future.sequence(res), 5.seconds).toMap

    // Return the saved offsets for the assigned partitions
    Offsets(offsetMap)
  }

  private def revokedListener(tps: List[TopicPartition]): Unit = {
    log.info("Partitions have been revoked" + tps.toString())
    // Opportunity to clear any state for the revoked partitions
    self ! PartitionsRevoked(tps)
    ()
  }
}