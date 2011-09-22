/**
 *  Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.cluster.metrics

/*
  When registered, MetricsAlterationMonitor is triggered, when the metrics (of a node, actor),
  matching predicate are generated
 */
trait MetricsAlterationMonitor[T] extends Comparable[MetricsAlterationMonitor[T]] {

  /*
     * Unique identifier of the monitor
     */
  def id: String

  def compareTo(otherMonitor: MetricsAlterationMonitor[T]) = id.compareTo(otherMonitor.id)

  /*
     * Defines conditions that must be satisfied in order to <code>react<code> on the changed metrics
     */
  def reactsOn(metrics: T): Boolean

  /*
     * Reacts on the changed metrics
     */
  def react(metrics: T): Unit

}

/*
  ClusterMetricsMonitor is being triggered with the metrics gathered at all running cluster nodes
 */
trait ClusterMetricsMonitor[T <: Metrics] extends MetricsAlterationMonitor[Array[T]]

/*
  ActorMetricsMonitor watches clustered actors with daemons running at different cluster nodes
 */
trait ActorMetricsMonitor extends ClusterMetricsMonitor[ActorMetrics] {

  def actorAddress: String

}