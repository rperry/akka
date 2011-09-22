/**
 *  Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.cluster.routing.leastram.homenode

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

object LeastRamHomeNodeMultiJvmSpec {

  val NrOfNodes = 2

  class SomeActor extends Actor with Serializable {
    def receive = {
      case "identify" ⇒ {
        self.reply(Config.nodename)
      }
    }
  }

}

class LeastRamHomeNodeMultiJvmNode1 extends MasterClusterTestNode {

  import LeastRamHomeNodeMultiJvmSpec._

  val testNodes = NrOfNodes

  "___" must {
    "___" in {

      Cluster.node.start()
      barrier("waiting-for-begin", NrOfNodes).await()
      barrier("waiting-for-end", NrOfNodes).await()

      node.shutdown()
    }
  }

}

class LeastRamHomeNodeMultiJvmNode2 extends ClusterTestNode {

  import LeastRamHomeNodeMultiJvmSpec._
  import Cluster._

  "Least Messages: A Router" must {
    "obey 'home-node' config option when instantiated actor in cluster" in {

      Cluster.node.start()
      barrier("waiting-for-begin", NrOfNodes).await()

      val actorNode1 = Actor.actorOf[SomeActor]("service-node1").start()
      val name1 = (actorNode1 ? "identify").get.asInstanceOf[String]
      name1 must equal("node1")

      val actorNode2 = Actor.actorOf[SomeActor]("service-node2").start()
      val name2 = (actorNode2 ? "identify").get.asInstanceOf[String]
      name2 must equal("node2")

      barrier("waiting-for-end", NrOfNodes).await()
      node.shutdown()
    }
  }

}