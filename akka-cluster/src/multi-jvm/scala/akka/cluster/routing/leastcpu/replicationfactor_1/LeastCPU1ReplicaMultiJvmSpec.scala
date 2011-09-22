/**
 *  Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.cluster.routing.leastcpu.replicationfactor_1

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

object LeastCPU1ReplicaMultiJvmSpec {

  class HelloWorld extends Actor with Serializable {
    def receive = {
      case "Hello" ⇒
        self.reply("World from node [" + Config.nodename + "]")
    }
  }

}

class LeastCPU1ReplicaMultiJvmNode1 extends MasterClusterTestNode {

  import LeastCPU1ReplicaMultiJvmSpec._
  import Cluster._

  val testNodes = 1

  "LeastCPU: A cluster" must {

    "send messages to the actor residing at the only running node" in {

      Cluster.node.start()

      var hello = Actor.actorOf[HelloWorld]("service-hello")
      hello must not equal (null)
      hello.address must equal("service-hello")
      hello.isInstanceOf[ClusterActorRef] must be(true)

      hello must not equal (null)
      val reply = (hello ? "Hello").as[String].getOrElse(fail("Should have recieved reply from node1"))
      reply must equal("World from node [node1]")

      node.shutdown()
    }
  }
}