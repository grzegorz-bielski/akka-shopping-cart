package shopping.cart

import scala.concurrent.Future
import org.slf4j.LoggerFactory
import akka.actor.typed.ActorSystem
import akka.util.Timeout
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import java.util.concurrent.TimeoutException
import akka.grpc.GrpcServiceException
import io.grpc.Status

class ShoppingCartServiceImpl(system: ActorSystem[_]) extends proto.ShoppingCartService {
  private val logger = LoggerFactory.getLogger(getClass)
  import system.executionContext

  implicit private val timeout =
    Timeout.create(system.settings.config.getDuration("shopping-cart-service.ask-timeout"))

  private val sharding = ClusterSharding(system)

  override def addItem(in: proto.AddItemRequest): Future[proto.Cart] = {
    logger.info("addItem {} to cart {}", in.itemId, in.cartId)

    sharding
      .entityRefFor(ShoppingCart.EntityKey, in.cartId)
      .askWithStatus(ShoppingCart.AddItem(in.itemId, in.quantity, _))
      .map(toProto)
      .recoverWith {
        case _: TimeoutException =>
          Future.failed(
            new GrpcServiceException(Status.UNAVAILABLE.withDescription("Operation timed out")))
        case exc =>
          Future.failed(
            new GrpcServiceException(Status.INVALID_ARGUMENT.withDescription(exc.getMessage)))
      }

  }

  private def toProto(cart: ShoppingCart.Summary): proto.Cart =
    proto.Cart(cart.items.iterator.map { case (itemId, quantity) =>
      proto.Item(itemId, quantity)
    }.toSeq)
}
