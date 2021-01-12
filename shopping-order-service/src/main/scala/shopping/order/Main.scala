package shopping.order

import akka.actor.typed.scaladsl.AbstractBehavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement

object Main {

  def main(args: Array[String]): Unit =
    ActorSystem[Nothing](Main(), "ShoppingOrderService")

  def apply(): Behavior[Nothing] =
    Behaviors.setup[Nothing](context => new Main(context))

}

class Main(context: ActorContext[Nothing])
    extends AbstractBehavior[Nothing](context) {
  val system = context.system
  val config = system.settings.config

  AkkaManagement(system).start()
  ClusterBootstrap(system).start()

  ShoppingOrderServer.start(
    interface = config.getString("shopping-order-service.grpc.interface"),
    port = config.getInt("shopping-order-service.grpc.port"),
    system,
    new ShoppingOrderServiceImpl)

  override def onMessage(msg: Nothing): Behavior[Nothing] =
    this
}
