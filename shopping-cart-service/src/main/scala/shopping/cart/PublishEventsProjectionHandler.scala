package shopping.cart

import akka.kafka.scaladsl.SendProducer
import akka.actor.typed.ActorSystem
import akka.projection.scaladsl.Handler
import akka.projection.eventsourced.EventEnvelope
import akka.Done
import scala.concurrent.Future
import org.apache.kafka.clients.producer.ProducerRecord
import com.google.protobuf.any.{ Any => ScalaPBAny }
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext

class PublishEventsProjectionHandler(
    system: ActorSystem[_],
    topic: String,
    sendProducer: SendProducer[String, Array[Byte]])
    extends Handler[EventEnvelope[ShoppingCart.Event]] {

  private val log = LoggerFactory.getLogger(getClass)
  private implicit val ec: ExecutionContext = system.executionContext

  override def process(envelope: EventEnvelope[ShoppingCart.Event]): Future[Done] = {
    val event = envelope.event
    val record = new ProducerRecord(topic, event.cartId, serialize(event))

    sendProducer.send(record).map { meta =>
      log.info("Published event [{}] to topic/partition {}/{}", event, topic, meta.partition)
      Done
    }
  }

  private def serialize(event: ShoppingCart.Event): Array[Byte] = {
    ScalaPBAny
      .pack(
        event match {
          case ShoppingCart.ItemAdded(cartId, itemId, quantity) =>
            proto.ItemAdded(cartId, itemId, quantity)
          case ShoppingCart.CheckedOut(cartId, _) =>
            proto.CheckedOut(cartId)
        },
        "shopping-cart-service")
      .toByteArray
  }
}
