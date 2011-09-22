package akka.cluster.routing.leastmessages.failover

import akka.config.Config
import akka.cluster._
import akka.actor.{ ActorRef, Actor }
import akka.event.EventHandler
import akka.testkit.{ EventFilter, TestEvent }
import akka.util.duration._
import akka.util.{ Duration, Timer }
import java.util.{ Collections, Set ⇒ JSet }
import java.net.ConnectException
import java.lang.Thread
import akka.cluster.LocalCluster._
import akka.dispatch.Futures
import scala.collection.JavaConverters._
import akka.cluster.ChangeListener.ChangeListener
import java.nio.channels.{ ClosedChannelException, NotYetConnectedException }
import akka.cluster.metrics.{ NodeMetrics, ClusterMetricsMonitor, ActorMetrics, ActorMetricsMonitor }
import scala.collection.SortedSet
import java.util.concurrent.{ CountDownLatch, TimeUnit }

object LeastMessagesFailoverMultiJvmSpec {

  val NrOfNodes = 3

  val refreshTimeout = 10.millis

  class SomeActor extends Actor with Serializable {

    def receive = {
      case "identify" ⇒
        Thread sleep refreshTimeout.toMillis * 10
        self.tryReply(Config.nodename)
    }
  }

}

class LeastMessagesFailoverMultiJvmNode1 extends MasterClusterTestNode {

  import LeastMessagesFailoverMultiJvmSpec._

  def testNodes = NrOfNodes

  "Least Messages: when least messages router fails" must {
    "jump to another replica" in {

      val ignoreExceptions = Seq(
        EventFilter[NotYetConnectedException],
        EventFilter[ConnectException],
        EventFilter[ClusterException],
        EventFilter[ClosedChannelException])

      var oldFoundConnections: JSet[String] = null
      var actor: ActorRef = null

      barrier("node-start", NrOfNodes) {
        EventHandler.notify(TestEvent.Mute(ignoreExceptions))
        Cluster.node.start()
        Cluster.node.metricsManager.refreshTimeout = refreshTimeout
      }

      barrier("actor-creation", NrOfNodes) {
        actor = Actor.actorOf[SomeActor]("service-hello")
        actor.isInstanceOf[ClusterActorRef] must be(true)
      }

      val timer = Timer(30.seconds, true)
      while (timer.isTicking &&
        !Cluster.node.isInUseOnNode("service-hello", "node1") &&
        !Cluster.node.isInUseOnNode("service-hello", "node3")) {}

      barrier("actor-usage", NrOfNodes) {
        Cluster.node.isInUseOnNode("service-hello") must be(true)
        identifyConnections(actor, Array("node1", "node3"))
      }

      barrier("fail", NrOfNodes).await()

      Thread.sleep(5000)

      barrier("verify-fail-over", NrOfNodes - 1) {
        var timer = Timer(30.seconds, true)
        while (timer.isTicking &&
          (!Cluster.node.isInUseOnNode("service-hello", "node1") ||
            !Cluster.node.isInUseOnNode("service-hello", "node2"))) {}
        //FIXME: randomly fails at failover
        //identifyConnections(actor, Array("node1", "node2"))
      }

      Cluster.node.shutdown()
    }
  }

  def identifyConnections(actor: ActorRef, nodes: Array[String]) = {
    val node1present = new CountDownLatch(1)
    val node2present = new CountDownLatch(1)

    var timer = Timer(30.seconds, true)
    while (timer.isTicking && (node1present.getCount == 1 || node2present.getCount == 1)) {
      Thread sleep refreshTimeout.toMillis * 3
      (actor ? "identify").map {
        case n if (n == nodes(0)) ⇒ node1present.countDown()
        case n if (n == nodes(1)) ⇒ node2present.countDown()
      }
    }

    node1present.await(5, TimeUnit.SECONDS) must be(true)
    node2present.await(5, TimeUnit.SECONDS) must be(true)
  }

}

class LeastMessagesFailoverMultiJvmNode2 extends ClusterTestNode {

  import LeastMessagesFailoverMultiJvmSpec._

  "___" must {
    "___" in {
      barrier("node-start", NrOfNodes) {
        Cluster.node.start()
        Cluster.node.metricsManager.refreshTimeout = refreshTimeout
      }

      barrier("actor-creation", NrOfNodes).await()
      barrier("actor-usage", NrOfNodes).await()

      Cluster.node.isInUseOnNode("service-hello") must be(false)

      barrier("fail", NrOfNodes).await()

      Thread.sleep(5000) // wait for fail-over from node3

      barrier("verify-fail-over", NrOfNodes - 1).await()
    }
  }

}

class LeastMessagesFailoverMultiJvmNode3 extends ClusterTestNode {

  import LeastMessagesFailoverMultiJvmSpec._

  "___" must {
    "___" in {
      barrier("node-start", NrOfNodes) {
        Cluster.node.start()
        Cluster.node.metricsManager.refreshTimeout = refreshTimeout
      }

      barrier("actor-creation", NrOfNodes).await()
      barrier("actor-usage", NrOfNodes).await()

      Cluster.node.isInUseOnNode("service-hello") must be(true)

      barrier("fail", NrOfNodes) {
        Cluster.node.shutdown()
      }

    }
  }
}

