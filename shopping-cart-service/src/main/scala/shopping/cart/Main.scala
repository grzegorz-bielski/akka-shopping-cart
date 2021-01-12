package shopping.cart

import akka.actor.typed.scaladsl.AbstractBehavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import akka.grpc.GrpcClientSettings
import shopping.order.proto.ShoppingOrderServiceClient

// ## write
// (outside)
// -------grpc----->
// <server>
// <shopping cart service>
// --msg(command)-->
// <persistent actor>
// ----msg(event)-->
// - event log (cassandra)
// - update projections (cassandra)

// ## read
// (outside)
// -------grpc----->
// <server>
// <shopping cart service>
// ( --msg(command ?)--> ) |  selectOne
// (<persistent actor> )   |  <cassandra connection>

object Main {
  def main(args: Array[String]): Unit =
    ActorSystem[Nothing](Main(), "ShoppingCartService")

  def apply() = Behaviors.setup[Nothing](new Main(_))
}

class Main(context: ActorContext[Nothing]) extends AbstractBehavior[Nothing](context) {
  val system = context.system

  initCluster()
  initActors()

  val itemPopularityRepo = initRepositories()
  val orderService = initServices()

  initProjections()
  initGrpcServer()

  override def onMessage(msg: Nothing): Behavior[Nothing] = this

  protected def initGrpcServer() = {
    val grpcInterface = system.settings.config.getString("shopping-cart-service.grpc.interface")
    val grpcPort = system.settings.config.getInt("shopping-cart-service.grpc.port")
    val grpcService = new ShoppingCartServiceImpl(system, itemPopularityRepo)

    SoppingCartServer.start(grpcInterface, grpcPort, system, grpcService)
  }

  protected def initCluster() = {
    AkkaManagement(system).start()
    ClusterBootstrap(system).start()
  }

  protected def initActors() = {
    ShoppingCart.init(system) // persistent actor
  }

  protected def initRepositories() = {
    implicit val ec = system.executionContext
    val session = CassandraSessionRegistry(system).sessionFor("akka.persistence.cassandra")
    val keySpace =
      system.settings.config.getString("akka.projection.cassandra.offset-store.keyspace")

    new ItemPopularityRepositoryImpl(session, keySpace)
  }

  protected def initServices() = {
    val settings = GrpcClientSettings
      .connectToServiceAt(
        system.settings.config.getString("shopping-order-service.host"),
        system.settings.config.getInt("shopping-order-service.port"))(system)
      .withTls(enabled = false) // should not turned off in prod)

    ShoppingOrderServiceClient(settings)(system)
  }

  protected def initProjections() = {
    ItemPopularityProjection.init(system, itemPopularityRepo)
    SendOrderProjection.init(system, orderService)
    PublishEventsProjection.init(system)
  }
}
