package shopping.cart

import akka.projection.scaladsl.Handler
import akka.projection.eventsourced.EventEnvelope
import org.slf4j.LoggerFactory
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.Done
import scala.concurrent.Future
import shopping.order.proto.Item
import akka.util.Timeout
import shopping.order.proto.OrderRequest
import shopping.order.proto.ShoppingOrderService

class SendOrderProjectionHandler(
    system: akka.actor.typed.ActorSystem[_],
    orderService: ShoppingOrderService)
    extends Handler[EventEnvelope[ShoppingCart.Event]] {
  private val log = LoggerFactory.getLogger(getClass)
  private implicit val ec = system.executionContext

  private val sharding = ClusterSharding(system)
  implicit private val timeout =
    Timeout.create(system.settings.config.getDuration("shopping-cart-service.ask-timeout"))

  override def process(envelope: EventEnvelope[ShoppingCart.Event]): Future[Done] =
    envelope.event match {
      case checkout: ShoppingCart.CheckedOut => sendOrder(checkout)
      case _ =>
        Future.successful(Done)
    }

  private def sendOrder(checkout: ShoppingCart.CheckedOut): Future[Done] = {
    sharding.entityRefFor(ShoppingCart.EntityKey, checkout.cartId).ask(ShoppingCart.Get).flatMap {
      cart =>
        val items = cart.items.iterator.map { case (id, quantity) => Item(id, quantity) }.toList

        log.info("Sending order of {} items for cart {}", items.size, checkout.cartId)

        // ShoppingOrderService must be idempotent -> at-least-once delivery
        orderService.order(OrderRequest(checkout.cartId, items)).map(_ => Done)
    }

  }
}
