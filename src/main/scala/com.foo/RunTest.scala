package com.foo

import akka.stream.scaladsl._
import akka.kafka.scaladsl._

import akka.kafka.ConsumerMessage._
import akka.kafka._

import akka.kafka.{ Subscriptions, ConsumerSettings }
import akka.kafka.ConsumerMessage.CommittableMessage
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, StringDeserializer }
import org.apache.kafka.clients.consumer.ConsumerConfig
import akka.stream._
import akka.actor._
import java.util.concurrent.LinkedBlockingQueue


case class RunTest(num:Int, producer:Producer, kafkaHost:String, groupId:String, topic:String)(implicit system:ActorSystem, mat:Materializer) {
  producer.populate(num, topic)
  Thread.sleep(2000)
  partitionInfo(topic)

  val count = new LinkedBlockingQueue[Int]()

  println("Consuming...")
  //
  // Define the flow using Graph DSL
  //
  val flow = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[akka.NotUsed] =>
    import GraphDSL.Implicits._
    type In = CommittableMessage[Array[Byte], String]

    val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers(kafkaHost)
      .withGroupId(groupId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val src = Consumer.committableSource(consumerSettings, Subscriptions.topics(topic))

    // Simulated load -- real project would have a workflow here
    val work = Flow[In].map { i => 
      i
    }.batch(max = 100, first => CommittableOffsetBatch.empty.updated(first.committableOffset)) { (batch, elem) =>
      count.add(1)
      batch.updated(elem.committableOffset)
    }

    val commit = Flow[CommittableOffsetBatch].map { msg =>
      msg.commitScaladsl()
    }

    src ~> work ~> commit ~> Sink.ignore

    ClosedShape
  })


  //
  //  The actual test run
  //
  Thread.sleep(3000)
  val now = System.currentTimeMillis()
  flow.run()
  while (count.size < num && System.currentTimeMillis() > now + 20000) Thread.sleep(500)  // Loop until done or timeout
  val later = System.currentTimeMillis()
  val tps = (later - now) / 1000.0

  Thread.sleep(15000)
  println(tps + " TPS")
  groupInfo(groupId)
  println("TOTAL: " + count.size)
  Thread.sleep(3000)

  private def partitionInfo(topic: String) =
    kafka.tools.GetOffsetShell.main(Array("--topic", topic, "--broker-list", kafkaHost, "--time", "-1"))
  private def groupInfo(group: String) =
    kafka.admin.ConsumerGroupCommand.main(Array("--describe", "--group", group, "--bootstrap-server", kafkaHost, "--new-consumer"))
}

