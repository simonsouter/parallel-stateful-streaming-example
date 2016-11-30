package cakesolutions.stream

import com.datastax.driver.core.{Cluster, ResultSet, Row, Session}
import org.slf4j.LoggerFactory
import troy.driver.DSL._
import troy.dsl._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object StateRepo {}

object OffsetAndState {
  def empty: OffsetAndState = {
    OffsetAndState(0, Map())
  }
}

case class OffsetAndState(offset: Long, state: Map[Int, MeterState])

//TODO state as blob
case class MeterState(samples: Int, total: Int) {
  def update(reading: Int) = copy(samples = this.samples + 1, total = this.total + reading)
}

case class TotalUsage(topic: String, partition: Int, meterId: Int, offset: Long, samples: Int, total: Int)

class StateRepo(host: String, port: Int) {
  private val log = LoggerFactory.getLogger(Main.getClass.getName)

  private val cluster =
    new Cluster.Builder().addContactPoints(host).withPort(port).build()

  implicit val session: Session = cluster.connect()

  private val offsetAndStateByKey: (String, Int) => Future[Seq[Row]] = withSchema {
    (topic: String, partition: Int) =>
      cql"""
        SELECT offset, meter_id, samples, total
        FROM meter.total_usage
        WHERE topic = $topic
        AND partition = $partition
    """.prepared.executeAsync.all
  }

  val save: (TotalUsage) => Future[ResultSet] = withSchema {
    (totalUsage: TotalUsage) =>
      cql"""
           INSERT INTO meter.total_usage (topic, partition, meter_id, offset, samples, total)
           VALUES ( ${totalUsage.topic}, ${totalUsage.partition}, ${totalUsage.meterId}, ${totalUsage.offset}, ${totalUsage.samples}, ${totalUsage.total});
         """.prepared.executeAsync
  }

  def findOffsetByKey(topic: String, partition: Int): Future[OffsetAndState] =
    offsetAndStateByKey(topic, partition).map { rows =>
      val tuples = rows.map { row =>
        (row.getLong(0), row.getInt(1), row.getInt(2), row.getInt(3))
      }

      if (tuples.nonEmpty) {
        OffsetAndState(tuples.head._1, tuples.groupBy(_._2).
          mapValues { state =>
            MeterState(state.head._3, state.head._4)
          })
      } else OffsetAndState.empty
    }

  def close(): Unit = {
    session.close()
    cluster.close()
  }
}