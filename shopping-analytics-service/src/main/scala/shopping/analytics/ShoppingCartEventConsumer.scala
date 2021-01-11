package shopping.analytics

import scala.concurrent.duration._
import org.slf4j.LoggerFactory
import akka.actor.typed.ActorSystem
import akka.kafka.ConsumerSettings
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import akka.kafka.CommitterSettings
import akka.stream.scaladsl.RestartSource
import akka.stream.RestartSettings
import akka.kafka.scaladsl.{ Committer, Consumer }
import akka.kafka.Subscriptions
import scala.concurrent.ExecutionContext
import org.apache.kafka.clients.consumer.ConsumerRecord
import com.google.protobuf.any.{ Any => ScalaPBAny }
import scala.util.Try
import scala.concurrent.Future
import scala.util.control.NonFatal
import akka.Done

object ShoppingCartEventConsumer {
  private val log = LoggerFactory.getLogger(getClass)

  def init(system: ActorSystem[_]): Unit = {
    implicit val sys = system
    implicit val ec = system.executionContext

    val topic = system.settings.config
      .getString("shopping-analytics-service.shopping-cart-kafka-topic")
    val consumerSettings = ConsumerSettings(
      system,
      new StringDeserializer,
      new ByteArrayDeserializer).withGroupId("shopping-cart-analytics")
    val committerSettings = CommitterSettings(system)

    RestartSource
      .onFailuresWithBackoff(
        RestartSettings(
          minBackoff = 1.seconds,
          maxBackoff = 30.seconds,
          randomFactor = 0.1)) { () =>
        Consumer
          .committableSource(consumerSettings, Subscriptions.topics(topic))
          .mapAsync(1) { msg =>
            handleRecord(msg.record).map(_ => msg.committableOffset)
          }
          .via(Committer.flow(committerSettings))
      }
      .run()
  }

  private def handleRecord(
      record: ConsumerRecord[String, Array[Byte]]): Future[Done] = {
    val bytes = record.value()
    val x = ScalaPBAny.parseFrom(bytes)
    val typeUrl = x.typeUrl

    try {
      val inputBytes = x.value.newCodedInput()
      val event =
        typeUrl match {
          case "shopping-cart-service/shoppingcart.ItemAdded" =>
            proto.ItemAdded.parseFrom(inputBytes)
          case "shopping-cart-service/shoppingcart.CheckedOut" =>
            proto.CheckedOut.parseFrom(inputBytes)
          case _ =>
            throw new IllegalArgumentException(
              s"unknown record type [$typeUrl]")
        }

      event match {
        case proto.ItemAdded(cartId, itemId, quantity, _) =>
          log.info("ItemAdded: {} {} to cart {}", quantity, itemId, cartId)
        case proto.CheckedOut(cartId, _) =>
          log.info("CheckedOut: cart {} checked out", cartId)
      }

      Future.successful(Done)
    } catch {
      case NonFatal(e) =>
        log.error("Could not process event of type [{}]", typeUrl, e)
        // continue with next
        Future.successful(Done)
    }
  }
}
