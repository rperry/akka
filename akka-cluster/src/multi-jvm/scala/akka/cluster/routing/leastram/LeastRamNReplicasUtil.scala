package akka.cluster.routing.leastram

import akka.actor.Actor
import akka.cluster.metrics.{ NodeMetrics, ClusterMetricsMonitor }
import akka.config.Config
import java.util.concurrent.CountDownLatch

object LeastRamNReplicasUtil {

  class MetricsExistenceMonitor(complete: CountDownLatch, nodesCount: Int) extends ClusterMetricsMonitor[NodeMetrics] {

    def id = "metricsExistenceMonitor"

    def reactsOn(metrics: Array[NodeMetrics]) = metrics.size == nodesCount

    def react(metrics: Array[NodeMetrics]) = complete.countDown()

  }

  class HelloWorld extends Actor with Serializable {
    def receive = {
      case msg ⇒ self tryReply Config.nodename
    }
  }

  def generateDummy(size: Int) =
    (1 to size) map (_ ⇒ <foo><foo><foo><foo><foo></foo></foo></foo></foo></foo>)

}
