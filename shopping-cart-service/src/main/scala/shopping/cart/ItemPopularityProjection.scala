package shopping.cart

import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import akka.projection.ProjectionBehavior
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.projection.cassandra.scaladsl.CassandraProjection
import akka.projection.ProjectionId
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings

object ItemPopularityProjection {
  val name = "ItemPopularityProjection"

  def init(system: ActorSystem[_], repository: ItemPopularityRepository) =
    ShardedDaemonProcess(system).init(
      name,
      ShoppingCart.tags.size,
      index => ProjectionBehavior(createProjectionFor(system, repository, index)),
      ShardedDaemonProcessSettings(system),
      Some(ProjectionBehavior.Stop))

  private def createProjectionFor(
      system: ActorSystem[_],
      repo: ItemPopularityRepository,
      index: Int) = {
    val tag = ShoppingCart.tags(index)
    val sourceProvider = EventSourcedProvider
      .eventsByTag[ShoppingCart.Event](system, CassandraReadJournal.Identifier, tag)

    CassandraProjection.atLeastOnce(
      ProjectionId(name, tag),
      sourceProvider,
      handler = () => new ItemPopularityProjectionHandler(tag, system, repo))
  }
}
