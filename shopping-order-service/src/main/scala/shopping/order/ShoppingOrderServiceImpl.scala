package shopping.order

import org.slf4j.LoggerFactory
import scala.concurrent.Future
import shopping.order.proto.{ OrderRequest, OrderResponse }

class ShoppingOrderServiceImpl extends proto.ShoppingOrderService {
  private val logger = LoggerFactory.getLogger(getClass)

  override def order(in: OrderRequest): Future[OrderResponse] = {
    val total = in.items.iterator.map(_.quantity).sum

    logger.info("Order {} items from cart {}.", total, in.cartId)

    Future.successful(OrderResponse(ok = true))
  }
}
