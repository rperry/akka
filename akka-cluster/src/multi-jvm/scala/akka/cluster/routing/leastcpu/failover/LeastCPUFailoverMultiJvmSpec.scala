package akka.cluster.routing.leastcpu.failover

import akka.config.Config
import akka.cluster._
import akka.actor.{ ActorRef, Actor }
import akka.event.EventHandler
import akka.util.duration._
import akka.util.{ Duration, Timer }
import akka.testkit.{ EventFilter, TestEvent }
import java.util.{ Collections, Set ⇒ JSet }
import java.net.ConnectException
import java.nio.channels.NotYetConnectedException
import akka.cluster.LocalCluster._

object LeastCPUFailoverMultiJvmSpec {

  val NrOfNodes = 3

  class SomeActor extends Actor with Serializable {

    def receive = {
      case "identify" ⇒
        self.reply(Config.nodename)
    }
  }

}

class LeastCPUFailoverMultiJvmNode1 extends MasterClusterTestNode {

  import LeastCPUFailoverMultiJvmSpec._

  def testNodes = NrOfNodes

  "Least CPU: when random router fails" must {
    "jump to another replica" in {
      val ignoreExceptions = Seq(
        EventFilter[NotYetConnectedException],
        EventFilter[ConnectException],
        EventFilter[ClusterException],
        EventFilter[java.nio.channels.ClosedChannelException])

      var oldFoundConnections: JSet[String] = null
      var actor: ActorRef = null

      barrier("node-start", NrOfNodes) {
        EventHandler.notify(TestEvent.Mute(ignoreExceptions))
        Cluster.node.start()
      }

      barrier("actor-creation", NrOfNodes) {
        actor = Actor.actorOf[SomeActor]("service-hello")
        actor.isInstanceOf[ClusterActorRef] must be(true)
      }

      val timer = Timer(30.seconds, true)
      while (timer.isTicking &&
        !Cluster.node.isInUseOnNode("service-hello", "node2") &&
        !Cluster.node.isInUseOnNode("service-hello", "node3")) {}

      barrier("actor-usage", NrOfNodes) {
        oldFoundConnections = identifyConnections(actor)

        //since we have replication factor 2
        oldFoundConnections.size() must be(2)
      }

      val timer2 = Timer(30.seconds, true)
      while (timer2.isTicking &&
        !Cluster.node.isInUseOnNode("service-hello")) {}

      Thread sleep 5000

      val newFoundConnections = identifyConnections(actor)

      newFoundConnections.size() must be(1)
      newFoundConnections.iterator().next() must be("node1")

      Cluster.node.shutdown()
    }
  }

  def identifyConnections(actor: ActorRef): JSet[String] = {
    val set = new java.util.HashSet[String]
    for (i ← 0 until 100) { // we should get hits from both nodes in 100 attempts, if not then not very random
      val value = (actor ? "identify").get.asInstanceOf[String]
      set.add(value)
    }
    set
  }
}

class LeastCPUFailoverMultiJvmNode2 extends ClusterTestNode {

  import LeastCPUFailoverMultiJvmSpec._

  "___" must {
    "___" in {
      barrier("node-start", NrOfNodes) {
        Cluster.node.start()
      }

      barrier("actor-creation", NrOfNodes).await()
      barrier("actor-usage", NrOfNodes).await()

      Cluster.node.isInUseOnNode("service-hello") must be(true)

      Cluster.node.shutdown()
    }
  }
}

class LeastCPUFailoverMultiJvmNode3 extends ClusterTestNode {

  import LeastCPUFailoverMultiJvmSpec._

  "___" must {
    "___" in {
      barrier("node-start", NrOfNodes) {
        Cluster.node.start()
      }

      barrier("actor-creation", NrOfNodes).await()
      barrier("actor-usage", NrOfNodes).await()

      Cluster.node.isInUseOnNode("service-hello") must be(true)

      Cluster.node.shutdown()
    }
  }
}

