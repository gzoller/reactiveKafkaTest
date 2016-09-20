package com.foo

package co.blocke
package latekafka

import org.scalatest._
import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializerSettings, ActorMaterializer }
import scala.sys.process._
import scala.util.Try

class KafkaSpec() extends FunSpec with Matchers with BeforeAndAfterAll {
  val topic = "lowercaseStrings"
  val groupId = "group1"
  val pwd = System.getProperty("user.dir")
  val extraPath = s"$pwd/src/test/resources/extra"
  var wid = "" // world id
  var dip = "" // docker ip
  var kafkaHost = "" // kafka host
  var zooHost = "" // zookeeper host
  val producer = Producer()

  implicit val as = ActorSystem("ReactiveKafka")
  implicit val m = ActorMaterializer(
    ActorMaterializerSettings(as)
      .withAutoFusing(false)
      .withInputBuffer(32, 32)
  )

  val props = Map("auto.offset.reset" -> "earliest")

  // Set server.propertise in /opt/kakfa in world server:
  override def beforeAll() {
    wid = worldWait()
    dip = getDockerIP()
    val ips = List(9092, 2181).map(p => s"$dip:" + getDockerPort(wid, p))
    kafkaHost = ips(0)
    zooHost = ips(1)
    println("KAFKA HOST: " + kafkaHost)
    println("ZOO   HOST: " + zooHost)
    Thread.sleep(9000) // let it all come up and settle down in World server
    producer.create(kafkaHost, zooHost, topic)
  }

  override def afterAll() {
    as.shutdown()
    s"docker kill $wid".!!
  }

  describe("Kafka Must...") {
    it("Is fast - Reactive-Kafka") {
      RunTest( 1000000, producer, kafkaHost, groupId, topic )
    }
   }

  def worldWait(limit: Int = 30) = {
    println("++ Starting the World")
    val worldId = s"$pwd/src/test/resources/startWorld.sh $extraPath".!!
    while (cheapGet(s"http://${getDockerIP()}:${getDockerPort(worldId, 80)}/status").isEmpty) { Thread.sleep(500) }
    println("++ World started")
    worldId
  }

  private def cheapGet(uri: String) = Try { scala.io.Source.fromURL(uri, "utf-8").mkString }.toOption
  /** Get a port mapping from Docker.  Must be called only from outside a Docker! Use portster inside. Basically here for testing. */
  private def getDockerPort(containerId: String, port: Int) = (s"docker port $containerId $port".!!).split(':')(1).trim
  /** Get the IP of the active Docker machine.  Must be called only from outside a Docker! Basically here for testing. */
  private def getDockerIP() = {
    val machineName = (Seq("sh", "-c", "docker-machine active 2>/dev/null").!!).trim
    (s"docker-machine ip $machineName".!!).trim
  }
}