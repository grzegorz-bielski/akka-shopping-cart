package shopping.cart

import akka.actor.typed.ActorSystem
import akka.kafka.scaladsl.SendProducer
import akka.kafka.ProducerSettings
import org.apache.kafka.common.serialization.{ ByteArraySerializer, StringSerializer }
import akka.actor.CoordinatedShutdown
import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import akka.projection.ProjectionBehavior
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.projection.cassandra.scaladsl.CassandraProjection
import akka.projection.ProjectionId

object PublishEventsProjection {
  val name = "PublishEventsProjection"

  def init(system: ActorSystem[_]): Unit = {
    val sendProducer = createProducer(system)
    val topic = system.settings.config.getString("shopping-cart-service.shopping-cart-kafka-topic")

    ShardedDaemonProcess(system).init(
      name = name,
      ShoppingCart.tags.size,
      index => ProjectionBehavior(createProjectionFor(system, topic, sendProducer, index)),
      ShardedDaemonProcessSettings(system),
      Some(ProjectionBehavior.Stop))
  }

  private def createProducer(system: ActorSystem[_]): SendProducer[String, Array[Byte]] = {
    val settings = ProducerSettings(system, new StringSerializer, new ByteArraySerializer)
    val producer = SendProducer(settings)(system)

    CoordinatedShutdown(system)
      .addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, "close-sendProducer") { () =>
        producer.close()
      }

    producer
  }

  private def createProjectionFor(
      system: ActorSystem[_],
      topic: String,
      producer: SendProducer[String, Array[Byte]],
      index: Int) = {
    val tag = ShoppingCart.tags(index)

    CassandraProjection.atLeastOnce(
      projectionId = ProjectionId(name, tag),
      sourceProvider = EventSourcedProvider
        .eventsByTag[ShoppingCart.Event](system, CassandraReadJournal.Identifier, tag),
      handler = () => new PublishEventsProjectionHandler(system, topic, producer))
  }
}
