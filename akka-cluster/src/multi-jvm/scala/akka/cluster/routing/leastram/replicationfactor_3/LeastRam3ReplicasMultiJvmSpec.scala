/**
 *  Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.cluster.routing.leastram.replicationfactor_3

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

object LeastRam3ReplicasMultiJvmSpec {
  val NrOfNodes = 3
}

class LeastRam3ReplicasMultiJvmNode1 extends MasterClusterTestNode {

  import LeastRam3ReplicasMultiJvmSpec._

  def testNodes: Int = NrOfNodes

  "___" must {
    "___" in {
      Cluster.node.start()

      barrier("start-node", NrOfNodes).await()

      barrier("cleanup-node3", NrOfNodes) {
        generateDummy(1000000)
        Thread sleep Cluster.node.metricsManager.refreshTimeout.toMillis
      }

      barrier("shutdown", NrOfNodes).await()

      node.shutdown()
    }
  }
}

class LeastRam3ReplicasMultiJvmNode2 extends ClusterTestNode {

  import LeastRam3ReplicasMultiJvmSpec._
  import Cluster._

  "LeastRam: A cluster" must {

    "distribute requests between 3 nodes with regards to available heap space" in {
      Cluster.node.start()
      generateDummy(1000000)

      barrier("start-node", NrOfNodes).await()

      var actor = Actor.actorOf[HelloWorld]("service-hello")

      val allActorsAvailable = new CountDownLatch(3)
      Cluster.node.metricsManager.addMonitor(new MetricsExistenceMonitor(allActorsAvailable, 3))

      allActorsAvailable.await(30, TimeUnit.SECONDS) must be(true)

      ((1 to 10) map (actor ? _)).map(_.mapTo[String].get).toSet must be(Set("node1"))

      barrier("cleanup-node3", NrOfNodes) {
        generateDummy(1000000)
        Thread sleep Cluster.node.metricsManager.refreshTimeout.toMillis
      }

      ((1 to 10) map (actor ? _)).map(_.mapTo[String].get).toSet must be(Set("node3"))

      barrier("shutdown", NrOfNodes).await()

      node.shutdown()
    }
  }
}

class LeastRam3ReplicasMultiJvmNode3 extends ClusterTestNode {

  import LeastRam3ReplicasMultiJvmSpec._
  import Cluster._

  "___" must {
    "___" in {
      Cluster.node.start()
      var dummy = generateDummy(1000000)

      barrier("start-node", NrOfNodes).await()

      barrier("cleanup-node3", NrOfNodes) {
        dummy = null
        System.gc()
        Thread sleep Cluster.node.metricsManager.refreshTimeout.toMillis
      }

      barrier("shutdown", NrOfNodes).await()

      node.shutdown()
    }
  }
}
