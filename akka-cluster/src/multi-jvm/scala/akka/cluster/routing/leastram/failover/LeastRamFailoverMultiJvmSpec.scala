/**
 *  Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.cluster.routing.leastram.failover

import akka.cluster._
import akka.actor._
import akka.config.Config
import Cluster._
import akka.cluster.LocalCluster._
import akka.util.Duration
import akka.util.duration._
import akka.dispatch.Futures
import java.util.concurrent.{ TimeUnit, CountDownLatch }
import akka.cluster.metrics.{ NodeMetrics, ClusterMetricsMonitor, ActorMetrics, ActorMetricsMonitor }
import scala.collection.JavaConverters._
import akka.cluster.routing.leastram.LeastRamNReplicasUtil._
import akka.util.Timer
import Timer._

object LeastRamFailoverMultiJvmSpec {
  val NrOfNodes = 3
}

class LeastRamFailoverMultiJvmNode1 extends MasterClusterTestNode {

  import LeastRamFailoverMultiJvmSpec._

  def testNodes: Int = NrOfNodes

  "___" must {
    "___" in {
      Cluster.node.start()
      generateDummy(1000000)

      barrier("start-node", NrOfNodes).await()

      barrier("init-actor", NrOfNodes).await()

      barrier("remove-node3", NrOfNodes) {
        generateDummy(1000000)
        Thread sleep Cluster.node.metricsManager.refreshTimeout.toMillis
      }

      barrier("shutdown", NrOfNodes - 1).await()

      node.shutdown()
    }
  }
}

class LeastRamFailoverMultiJvmNode2 extends ClusterTestNode {

  import LeastRamFailoverMultiJvmSpec._
  import Cluster._

  "LeastRam: A cluster" must {

    "distribute requests between 3 nodes with regards to available heap space" in {

      Cluster.node.start()

      barrier("start-node", NrOfNodes).await()

      var actor: ActorRef = null

      barrier("init-actor", NrOfNodes) {
        actor = Actor.actorOf[HelloWorld]("service-hello")
      }

      val allNodesAvailable = new CountDownLatch(3)
      Cluster.node.metricsManager.addMonitor(new MetricsExistenceMonitor(allNodesAvailable, 3))

      val timer1 = Timer(30.seconds, true)
      while (timer1.isTicking &&
        (!Cluster.node.isInUseOnNode("service-hello", "node1") ||
          !Cluster.node.isInUseOnNode("service-hello", "node3"))) {}

      allNodesAvailable.await(30, TimeUnit.SECONDS) must be(true)

      ((1 to 10) map (actor ? _)).map(_.mapTo[String].get).toSet must be(Set("node3"))

      barrier("remove-node3", NrOfNodes).await()

      Thread sleep 5000

      val timer2 = Timer(30.seconds, true)
      while (timer2.isTicking &&
        !Cluster.node.isInUseOnNode("service-hello", "node1") &&
        !Cluster.node.isInUseOnNode("service-hello", "node2")) {}

      ((1 to 10) map (actor ? _)).map(_.mapTo[String].get).toSet must be(Set("node2"))

      barrier("shutdown", NrOfNodes - 1).await()

      node.shutdown()
    }
  }
}

class LeastRamFailoverMultiJvmNode3 extends ClusterTestNode {

  import LeastRamFailoverMultiJvmSpec._
  import Cluster._

  "___" must {
    "___" in {
      Cluster.node.start()

      barrier("start-node", NrOfNodes).await()

      barrier("init-actor", NrOfNodes).await()

      barrier("remove-node3", NrOfNodes).await()

      node.shutdown()
    }
  }
}
