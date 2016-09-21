package com.foo

import akka.stream.scaladsl._
import akka.kafka.scaladsl._

import akka.kafka.ConsumerMessage._
import akka.kafka._

import akka.kafka.{ Subscriptions, ConsumerSettings }
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, StringDeserializer }
import org.apache.kafka.clients.consumer.ConsumerConfig
import akka.stream._
import akka.actor._
import scala.concurrent.ExecutionContext.Implicits.global

case class RunTest2(num: Int, producer: Producer, kafkaHost: String, groupId: String, topic: String)(implicit system: ActorSystem, mat: Materializer) {

  // Pre-populate a topic w/some records (1 million)
  producer.populate(num, topic)
  Thread.sleep(2000)
  partitionInfo(topic)

  var count = 0 //new LinkedBlockingQueue[Int]()

  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
    .withBootstrapServers(kafkaHost)
    .withGroupId(groupId)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val done =
    Consumer.committableSource(consumerSettings, Subscriptions.topics(topic))
      .mapAsync(1) { msg =>
        count += 1
        scala.concurrent.Future(msg.committableOffset)
      }
      .batch(max = 20, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
        batch.updated(elem)
      }
      .mapAsync(3)(_.commitScaladsl())
      .runWith(Sink.ignore)

  var halfSecCount = 0
  while (count < num && halfSecCount < 120) { // Wait for up to a minute
    Thread.sleep(500)
    halfSecCount += 1
  }

  println("Done!")
  println("COUNT: " + count)
  Thread.sleep(5000)
  groupInfo(groupId)

  private def partitionInfo(topic: String) =
    kafka.tools.GetOffsetShell.main(Array("--topic", topic, "--broker-list", kafkaHost, "--time", "-1"))
  private def groupInfo(group: String) =
    kafka.admin.ConsumerGroupCommand.main(Array("--describe", "--group", group, "--bootstrap-server", kafkaHost, "--new-consumer"))

}