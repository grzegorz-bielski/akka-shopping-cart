package shopping.cart

import akka.actor.typed.scaladsl.AbstractBehavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry

// (outside)
// -------grpc----->
// <server>
// <shopping cart service>
// --msg(command)-->
// <persistent actor>
// ----msg(event)-->
// - event log (cassandra)
// - update projections (cassandra)

object Main {
  def main(args: Array[String]): Unit =
    ActorSystem[Nothing](Main(), "ShoppingCartService")

  def apply() = Behaviors.setup[Nothing](new Main(_))
}

class Main(context: ActorContext[Nothing]) extends AbstractBehavior[Nothing](context) {
  val system = context.system

  initCluster(system)
  initActors(system)
  val popularityRepo = initProjections(system)
  initGrpcServer(system, popularityRepo)

  override def onMessage(msg: Nothing): Behavior[Nothing] = this

  private def initGrpcServer(system: ActorSystem[_], repo: ItemPopularityRepository) = {
    val grpcInterface = system.settings.config.getString("shopping-cart-service.grpc.interface")
    val grpcPort = system.settings.config.getInt("shopping-cart-service.grpc.port")
    val grpcService = new ShoppingCartServiceImpl(system, repo)

    SoppingCartServer.start(grpcInterface, grpcPort, system, grpcService)
  }

  private def initCluster(system: ActorSystem[_]) = {
    AkkaManagement(system).start()
    ClusterBootstrap(system).start()
  }

  private def initActors(system: ActorSystem[_]) = {
    ShoppingCart.init(system) // persistent actor
  }

  private def initProjections(system: ActorSystem[_]) = {
    implicit val ec = system.executionContext
    val session = CassandraSessionRegistry(system).sessionFor("akka.persistence.cassandra")
    val keySpace =
      system.settings.config.getString("akka.projection.cassandra.offset-store.keyspace")
    val repository = new ItemPopularityRepositoryImpl(session, keySpace)

    ItemPopularityProjection.init(system, repository)

    repository
  }
}
