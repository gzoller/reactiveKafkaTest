package akka.foo

import scala.concurrent.{ Await, Promise }
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, StringDeserializer }
import akka.actor.ActorSystem
import org.apache.kafka.clients.consumer.ConsumerConfig
import akka.kafka.{ Subscriptions, ConsumerSettings }
import akka.stream.Materializer
import scala.concurrent.duration._
import akka.stream.scaladsl._
import akka.kafka.scaladsl._

import akka.stream.scaladsl._
import akka.kafka.scaladsl._
import akka.dispatch.ExecutionContexts
import scala.util.Success

//---
import akka.kafka.ConsumerMessage._
import akka.kafka._
import scala.concurrent._
import akka.kafka.{ Subscriptions, ConsumerSettings }
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, StringDeserializer }
import org.apache.kafka.clients.consumer.ConsumerConfig
import akka.stream._
import akka.actor._
import scala.concurrent.duration._

case class RunTest3(msgCount: Int, producer: com.foo.Producer, kafkaHost: String, groupId: String, topic: String)(implicit system: ActorSystem) {

  // Pre-populate a topic w/some records (1 million)
  producer.populate(msgCount, topic)
  Thread.sleep(2000)
  partitionInfo(topic)
  val partitionTarget = msgCount - 1

  val settings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
    .withBootstrapServers(kafkaHost)
    .withGroupId(groupId)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  def consumerAtLeastOnceBatched(batchSize: Int)(implicit mat: Materializer): Unit = {
    val promise = Promise[Unit]
    val control = Consumer.committableSource(settings, Subscriptions.topics(topic))
      .map {
        msg => msg.committableOffset
      }
      .batch(batchSize.toLong, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
        batch.updated(elem)
      }
      .mapAsync(3) { m =>
        m.commitScaladsl().map(_ => m)(ExecutionContexts.sameThreadExecutionContext)
      }
      .toMat(Sink.foreach { batch =>
        if (batch.offsets().head._2 >= partitionTarget)
          promise.complete(Success(()))
      })(Keep.left)
      .run()

    println("Control is: " + control.getClass.getName)
    val now = System.currentTimeMillis()
    Await.result(promise.future, 30.seconds)
    val later = System.currentTimeMillis()
    println("TPS: " + (msgCount / ((later - now) / 1000.0)))
    control.shutdown()

    groupInfo(groupId)
  }

  private def partitionInfo(topic: String) =
    kafka.tools.GetOffsetShell.main(Array("--topic", topic, "--broker-list", kafkaHost, "--time", "-1"))
  private def groupInfo(group: String) =
    kafka.admin.ConsumerGroupCommand.main(Array("--describe", "--group", group, "--bootstrap-server", kafkaHost, "--new-consumer"))

}