/**
 *  Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.cluster.routing.leastram.replicationfactor_1

import akka.cluster._
import akka.actor._
import akka.config.Config
import Cluster._
import akka.cluster.LocalCluster._
import akka.util.Duration
import akka.util.duration._
import akka.dispatch.Futures
import akka.cluster.metrics.{ NodeMetrics, ClusterMetricsMonitor, ActorMetrics, ActorMetricsMonitor }
import scala.collection.JavaConverters._
import akka.cluster.routing.leastram.LeastRamNReplicasUtil._
import java.util.concurrent.{ CountDownLatch, TimeUnit }

object LeastRam1ReplicaMultiJvmSpec {
  val NrOfNodes = 1
}

class LeastRam1ReplicaMultiJvmNode1 extends MasterClusterTestNode {

  import LeastRam1ReplicaMultiJvmSpec._
  import Cluster._

  def testNodes: Int = NrOfNodes

  "LeastRam: A cluster" must {

    "work consistently, if there's the only node" in {

      Cluster.node.start()

      var actor = Actor.actorOf[HelloWorld]("service-hello")

      val resultsCount = new CountDownLatch(10)

      ((1 to 10) map (actor ? _)).foreach(_.map { res ⇒
        resultsCount.countDown()
        res.toString must be("node1")
      })

      resultsCount.await(10, TimeUnit.SECONDS) must be(true)

      node.shutdown()
    }
  }
}
