/**
 *  Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.cluster.metrics

import akka.cluster._
import Cluster._
import akka.cluster.zookeeper._
import akka.actor._
import Actor._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.util.concurrent.{ ConcurrentHashMap, ConcurrentSkipListSet }
import java.util.concurrent.atomic.AtomicReference
import akka.util.{ Duration, Switch }
import akka.util.Helpers._
import akka.util.duration._
import akka.event.EventHandler
import org.apache.zookeeper.data.Stat
import org.I0Itec.zkclient.exception.{ ZkNodeExistsException, ZkNoNodeException }
import org.apache.zookeeper.CreateMode

/*
 * Basic metrics of an actor
 * @param nodeName name of the node the actor is running at
 * @param actorAddress address of the actor
 * @param mailboxSize number of messages in the mailbox to be processed by the actor
 */
case class DefaultActorMetrics(nodeName: String,
                               actorAddress: String,
                               mailboxSize: Long) extends ActorMetrics

/*
 * Instance of the metrics manager running on the node. To keep the fine performance, metrics of all the 
 * nodes in the cluster are cached internally, and refreshed from monitoring MBeans / Sigar (when if's local node),
 * of ZooKeeper (if it's metrics of all the nodes in the cluster) after a specified timeout - 
 * <code>metricsRefreshTimeout</code>
 * <code>metricsRefreshTimeout</code> defaults to 2 seconds, and can be declaratively defined through
 * akka.conf:
 * 
 * @exampl {{{
 *      akka.cluster.metrics-refresh-timeout = 2
 * }}}
 */
class LocalNodeMetricsManager(zkClient: AkkaZkClient, private val metricsRefreshTimeout: Duration)
  extends NodeMetricsManager {

  private case class ActorAddress(nodeName: String, actorAddress: String)

  /*
     * Provides metrics of the system that the node is running on, through monitoring MBeans, Hyperic Sigar
     * and other systems
     */
  lazy private val metricsProvider = SigarMetricsProvider(refreshTimeout.toMillis.toInt) fold ((thrw) ⇒ {
    EventHandler.warning(this, """Hyperic Sigar library failed to load due to %s: %s.
All the metrics will be retreived from monitoring MBeans, and may be incorrect at some platforms.
In order to get better metrics, please put "sigar.jar" to the classpath, and add platform-specific native libary to "java.library.path"."""
      .format(thrw.getClass.getName, thrw.getMessage))
    new JMXMetricsProvider
  },
    sigar ⇒ sigar)

  /*
     *  Metrics of all nodes in the cluster
     */
  private val localNodeMetricsCache = new ConcurrentHashMap[String, NodeMetrics]

  /*
     *  Metrics of the watched actors in the cluster
     */
  private val localActorMetricsCache = new ConcurrentHashMap[String, Array[ActorMetrics]]

  @volatile
  private var _refreshTimeout = metricsRefreshTimeout

  /* 
     * Plugged monitors (both local and cluster-wide)
     */
  private val alterationMonitors = new ConcurrentSkipListSet[MetricsAlterationMonitor[_]]

  private val _isRunning = new Switch(false)

  /*
     * If the value is <code>true</code>, metrics manages is started and running. Stopped, otherwise
     */
  def isRunning = _isRunning.isOn

  /*
     * Starts metrics manager. When metrics manager is started, it refreshes cache from ZooKeeper 
     * after <code>refreshTimeout</code>, and invokes plugged monitors
     */
  def start() = {
    _isRunning.switchOn { refresh() }
    this
  }

  /*
   * Path to the node metrics in the ZooKeeper
   */
  @inline
  private def metricsForNode(nodeName: String): String = "%s/%s".format(node.NODE_METRICS, nodeName)

  /*
   * Path to the actor metrics sub-tree in the ZooKeeper
   * Sub-tree contains metrics for the actor per nodes: actorAddress -> nodeName -> <metrics>
   */
  @inline
  private def metricsForActor(actorAddress: String): String = "%s/%s".format(node.ACTOR_METRICS, actorAddress)

  /*
   * Path to the actor metrics on a specified node in ZooKeeper
   */
  @inline
  private def metricsForActorOnNode(actorAddress: String, nodeName: String): String = "%s/%s".format(metricsForActor(actorAddress), nodeName)

  /*
   * Gets metrics of a locally running actors
   * @return None, if the actor has not be found in the local registry
   */
  private def getLocalActorMetrics(actorAddress: String) = {
    val actor = Actor.registry.actorFor(actorAddress)

    if (actor.isDefined) {
      val localActorRef = actor.get.asInstanceOf[LocalActorRef]
      Some(DefaultActorMetrics(nodeAddress.nodeName,
        actorAddress,
        localActorRef.dispatcher.mailboxSize(localActorRef)))
    } else None
  }

  /*
     * Adds monitor that reacts, when specific conditions are satisfied
     * When an actor monitor is added, cluster nodes start to gather and publish actor metrics
     */
  def addMonitor[T](monitor: MetricsAlterationMonitor[T]) = {
    alterationMonitors add monitor
    monitor match {
      case actorMonitor: ActorMetricsMonitor ⇒
        if (!zkClient.exists(metricsForActor(actorMonitor.actorAddress)))
          ignore[ZkNodeExistsException] {
            zkClient.create(metricsForActor(actorMonitor.actorAddress), null, CreateMode.PERSISTENT)
          }
      case unknown ⇒ EventHandler.debug(this, "Attempted to remove monitor of an unknown type: %s" format (unknown))
    }
  }

  /*
     * Remove monitor
     * When an actor monitor is removed, the actor is stopped being watched (nodes stop
     * gathering/publishing metrics for that actor)
     */
  def removeMonitor[T](monitor: MetricsAlterationMonitor[T]) = {
    alterationMonitors remove monitor
    monitor match {
      case actorMonitor: ActorMetricsMonitor ⇒
        ignore[ZkNodeExistsException](zkClient.delete(metricsForActor(actorMonitor.actorAddress)))
      case unknown ⇒ EventHandler.debug(this, "Attempted to remove monitor of an unknown type: %s" format (unknown))
    }
  }

  /*
   * Returns list of all the actors that need to be monitored. Metrics are gathered/published
   * only for monitored actors
   */
  @inline
  private def monitoredActors = zkClient.getChildren(node.ACTOR_METRICS)

  def refreshTimeout_=(newValue: Duration) = _refreshTimeout = newValue

  /*
     * Timeout after which metrics, cached in the metrics manager, will be refreshed from ZooKeeper
     */
  def refreshTimeout = _refreshTimeout

  @inline
  private def createOrStoreInZK[T](path: String, value: T) =
    if (!zkClient.exists(path))
      try zkClient.createEphemeral(path, value)
      catch {
        case e: ZkNodeExistsException ⇒ zkClient.writeData(path, value)
      }
    else zkClient.writeData(path, value)

  /*
     * Stores metrics of the node in ZooKeeper
     */
  private[akka] def storeMetricsInZK(metrics: Metrics) =
    createOrStoreInZK(metrics match {
      case nm: NodeMetrics  ⇒ metricsForNode(nm.nodeName)
      case am: ActorMetrics ⇒ metricsForActorOnNode(am.actorAddress, am.nodeName)
    }, metrics)

  /*
     * Gets metrics of the node from ZooKeeper
     */
  private[akka] def getMetricsFromZK[T](nodeName: String, actorAddress: Option[String] = None): T =
    zkClient.readData[T]((nodeName, actorAddress) match {
      case (nn, None)     ⇒ metricsForNode(nn)
      case (nn, Some(aa)) ⇒ metricsForActorOnNode(aa, nn)
    })

  /*
     * Removed metrics of the node from local cache and ZooKeeper
     */
  def removeNodeMetrics(nodeName: String) = {
    val metricsPath = metricsForNode(nodeName)
    if (zkClient.exists(metricsPath)) {
      ignore[ZkNoNodeException](zkClient.delete(metricsPath))
    }

    localNodeMetricsCache.remove(nodeName)
  }

  /*
     * Gets metrics of a local node directly from JMX monitoring beans/Hyperic Sigar
     */
  def getLocalMetrics = metricsProvider.getLocalMetrics

  /*
     * Gets metrics of the node, specified by the name. If <code>useCached</code> is true (default value),
     * metrics snapshot is taken from the local cache; otherwise, it's retreived from ZooKeeper'
     */
  def getMetrics(nodeName: String, useCached: Boolean = true): Option[NodeMetrics] =
    if (useCached)
      Option(localNodeMetricsCache.get(nodeName))
    else
      try {
        Some(getMetricsFromZK(nodeName))
      } catch {
        case ex: ZkNoNodeException ⇒ None
      }

  /*
     * Return metrics of all nodes in the cluster from ZooKeeper
     */
  private[akka] def getAllMetricsFromZK: Iterable[NodeMetrics] =
    zkClient.getChildren(node.NODE_METRICS)
      .map(nodeName ⇒ getMetricsFromZK[NodeMetrics](nodeName))

  private[akka] def getAllActorMetricsFromZK: Iterable[ActorMetrics] =
    for {
      actorAddress ← zkClient.getChildren(node.ACTOR_METRICS)
      nodeName ← zkClient.getChildren(metricsForActor(actorAddress))
    } yield getMetricsFromZK[ActorMetrics](nodeName, actorAddress)

  /*
     * Gets cached metrics of all nodes in the cluster
     */
  def getAllMetrics: Array[NodeMetrics] = localNodeMetricsCache.values.asScala.toArray

  /*
     * Refreshes locally cached metrics from ZooKeeper, and invokes plugged monitors
     */
  private[akka] def refresh(): Unit = {

    // stores metrics of a local node in ZK
    storeMetricsInZK(getLocalMetrics)

    // stores metrics of a monitored actors in ZK
    monitoredActors.foreach { getLocalActorMetrics(_).map(storeMetricsInZK) }

    refreshNodeMetricsCacheFromZK()
    refreshActorMetricsFromZK()

    if (isRunning) {
      Scheduler.scheduleOnce({ () ⇒ refresh() }, refreshTimeout.length, refreshTimeout.unit)
      invokeMonitors()
    }
  }

  private def refreshActorMetricsFromZK(): Unit = {
    val allActorMetricsFromZK = getAllActorMetricsFromZK.groupBy(_.actorAddress)

    localActorMetricsCache.keySet.foreach { key ⇒
      if (!allActorMetricsFromZK.contains(key))
        localActorMetricsCache.remove(key)
    }

    // RACY: to provide better performance, cache map is not locked during update, which may lead
    // to insignificant races, and monitors being stuffed with stale metrics (during 1 loop)
    allActorMetricsFromZK map {
      case (actorAddress, metrics) ⇒
        localActorMetricsCache.put(actorAddress, metrics.toArray)
    }
  }

  /*
     * Refreshes metrics manager cache from ZooKeeper
     */
  private def refreshNodeMetricsCacheFromZK(): Unit = {
    val allNodeMetricsFromZK = getAllMetricsFromZK.map { metrics ⇒ (metrics.nodeName, metrics) }

    localNodeMetricsCache.keySet.foreach { key ⇒
      if (!allNodeMetricsFromZK.contains(key))
        localNodeMetricsCache.remove(key)
    }

    // RACY: metrics for the node might have been removed both from ZK and local cache by the moment,
    // but will be re-cached, since they're still present in allMetricsFromZK snapshot. Not important, because 
    // cache will be fixed soon, at the next iteration of refresh
    allNodeMetricsFromZK map {
      case (node, metrics) ⇒
        localNodeMetricsCache.put(node, metrics)
    }
  }

  /*
     * Invokes monitors with the cached metrics
     */
  private def invokeMonitors(): Unit = if (!alterationMonitors.isEmpty) {

    // RACY: metrics for some nodes might have been removed/added by that moment. Not important,
    // because monitors will be fed with up-to-date metrics shortly, at the next iteration of refresh
    val clusterNodesMetrics = getAllMetrics
    val localNodeMetrics = clusterNodesMetrics.find(_.nodeName == nodeAddress.nodeName)

    val iterator = alterationMonitors.iterator

    @inline
    def invokeMonitor[T](monitor: MetricsAlterationMonitor[T]) = {
      @inline
      def invokeMonitor[T](monitor: MetricsAlterationMonitor[T], metrics: T) =
        if (monitor.reactsOn(metrics)) monitor.react(metrics)

      try monitor match {
        case actorMonitor: ActorMetricsMonitor ⇒
          val actorMetrics = localActorMetricsCache.get(actorMonitor.actorAddress)
          if (actorMetrics != null) invokeMonitor(actorMonitor, actorMetrics)
        case clusterMonitor: ClusterMetricsMonitor[_] ⇒
          invokeMonitor(clusterMonitor.asInstanceOf[ClusterMetricsMonitor[NodeMetrics]], clusterNodesMetrics)
        case nodeMonitor: MetricsAlterationMonitor[_] ⇒
          localNodeMetrics.map(invokeMonitor(nodeMonitor.asInstanceOf[MetricsAlterationMonitor[NodeMetrics]], _))
      } catch {
        case e ⇒ EventHandler.error(e, this, "Failed to invoke monitor %s".format(monitor))
      }
    }

    // RACY: there might be new monitors added after the iterator has been obtained. Not important, 
    // because refresh interval is meant to be very short, and all the new monitors will be called ad the
    // next refresh iteration
    while (iterator.hasNext) { invokeMonitor(iterator.next) }
  }

  def stop() = _isRunning.switchOff

}
