/**
 *  Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.cluster.routing.leastmessages.replicationfactor_3

import akka.cluster._
import akka.actor._
import akka.config.Config
import Cluster._
import akka.cluster.LocalCluster._
import akka.util.Duration
import akka.util.duration._
import akka.cluster.metrics.{ ActorMetrics, ActorMetricsMonitor }
import java.util.concurrent.CountDownLatch
import akka.dispatch.Futures

object LeastMessages3ReplicasMultiJvmSpec {
  val NrOfNodes = 3

  case class GenerateDummyMessages(nodeName: String, dummiesCount: Int)

  // Reacts when metrics are available at all nodes
  class MetricsExistenceMonitor(complete: CountDownLatch) extends ActorMetricsMonitor {

    def actorAddress = "service-hello"

    def id = "actorMetricsAllNodes"

    def reactsOn(metrics: Array[ActorMetrics]) = metrics.size == 3

    def react(metrics: Array[ActorMetrics]) = complete.countDown()

  }

  class HelloWorld extends Actor with Serializable {
    def receive = {
      case GenerateDummyMessages(nodeName, dummiesCount) if (nodeName.equals(nodeAddress.nodeName)) ⇒
        (1 to dummiesCount) foreach { _ ⇒ self ! "Dummy" }
      case "Dummy" ⇒ Thread sleep 5000
      case "Hello" ⇒ self.tryReply("World from node [" + Config.nodename + "]")
      case _       ⇒
    }
  }
}

class LeastMessages3ReplicasMultiJvmNode1 extends MasterClusterTestNode {

  import LeastMessages3ReplicasMultiJvmSpec._

  def testNodes: Int = NrOfNodes

  "___" must {
    "___" in {
      Cluster.node.start()

      barrier("start-nodes", NrOfNodes).await()

      barrier("create-actor", NrOfNodes).await()

      val allMetricsExist = new CountDownLatch(2)
      node.metricsManager.addMonitor(new MetricsExistenceMonitor(allMetricsExist))

      allMetricsExist.await()
      barrier("all-metrics-loaded", NrOfNodes).await()

      barrier("end-test", NrOfNodes).await()

      node.shutdown()
    }
  }
}

class LeastMessages3ReplicasMultiJvmNode2 extends ClusterTestNode {

  import LeastMessages3ReplicasMultiJvmSpec._
  import Cluster._

  "LeastMessages: A cluster" must {

    "distribute requests between 3 nodes with regards to the mailbox capacity" in {
      Cluster.node.start()

      barrier("start-nodes", NrOfNodes).await()

      var actor = Actor.actorOf[HelloWorld]("service-hello")

      barrier("create-actor", NrOfNodes).await()

      (1 to 3) foreach { _ ⇒
        actor ! GenerateDummyMessages("node1", 20)
        actor ! GenerateDummyMessages("node2", 20)
      }

      val allMetricsExist = new CountDownLatch(2)
      node.metricsManager.addMonitor(new MetricsExistenceMonitor(allMetricsExist))

      allMetricsExist.await()
      barrier("all-metrics-loaded", NrOfNodes).await()

      (1 to 10) map { _ ⇒ (actor ? "Hello").get } foreach (_ must be("World from node [node3]"))

      barrier("end-test", NrOfNodes).await()

      node.shutdown()
    }
  }
}

class LeastMessages3ReplicasMultiJvmNode3 extends ClusterTestNode {

  import LeastMessages3ReplicasMultiJvmSpec._
  import Cluster._

  "___" must {
    "___" in {
      Cluster.node.start()

      barrier("start-nodes", NrOfNodes).await()

      barrier("create-actor", NrOfNodes).await()

      val allMetricsExist = new CountDownLatch(2)
      node.metricsManager.addMonitor(new MetricsExistenceMonitor(allMetricsExist))

      allMetricsExist.await()
      barrier("all-metrics-loaded", NrOfNodes).await()

      barrier("end-test", NrOfNodes).await()

      node.shutdown()
    }
  }
}
