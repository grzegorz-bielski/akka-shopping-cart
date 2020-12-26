package shopping.cart

import akka.actor.typed.ActorRef
import akka.pattern.StatusReply
import scala.concurrent.duration._
import akka.persistence.typed.scaladsl.ReplyEffect
import akka.persistence.typed.scaladsl.Effect
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.RetentionCriteria
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.ActorSystem

object ShoppingCart {
  // commands
  sealed trait Command extends CborSerializable

  final case class AddItem(itemId: String, quantity: Int, replyTo: ActorRef[StatusReply[Summary]])
      extends Command

  final case class Summary(items: Map[String, Int]) extends CborSerializable

  // events

  sealed trait Event extends CborSerializable {
    def cartId: String
  }

  final case class ItemAdded(cartId: String, itemId: String, quantity: Int) extends Event

  // state

  object State {
    val empty = State(items = Map.empty)
  }

  final case class State(items: Map[String, Int]) extends CborSerializable {
    def hasItem(itemId: String) = items.contains(itemId)
    def isEmpty = items.isEmpty
    def updateItem(itemId: String, quantity: Int) = quantity match {
      case 0 => copy(items = items - itemId)
      case _ => copy(items = items + (itemId -> quantity))
    }
  }

  // init
  val EntityKey = EntityTypeKey[Command]("ShoppingCart")

  def start(system: ActorSystem[_]) =
    ClusterSharding(system).init(Entity(EntityKey)(ctx => ShoppingCart(ctx.entityId)))

  def apply(cartId: String) =
    EventSourcedBehavior
      .withEnforcedReplies[Command, Event, State](
        persistenceId = PersistenceId(EntityKey.name, cartId),
        emptyState = State.empty,
        commandHandler = (state, command) => handleCommand(cartId, state, command),
        eventHandler = (state, event) => handleEvent(state, event))
      .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 100, keepNSnapshots = 3))
      .onPersistFailure(SupervisorStrategy
        .restartWithBackoff(minBackoff = 200.millis, maxBackoff = 5.seconds, randomFactor = 0.1))

  // behaviors

  private def handleCommand(
      cartId: String,
      state: State,
      command: Command): ReplyEffect[Event, State] =
    command match {
      case AddItem(itemId, quantity, replyTo) =>
        if (state.hasItem(itemId))
          Effect.reply(replyTo)(
            StatusReply.Error(s"Item $itemId was already added to this shopping cart"))
        else if (quantity <= 0)
          Effect.reply(replyTo)(StatusReply.Error("Quantity must be greater than zero"))
        else
          Effect.persist(ItemAdded(cartId, itemId, quantity)).thenReply(replyTo) { cart =>
            StatusReply.Success(Summary(cart.items))
          }
    }

  private def handleEvent(state: State, event: Event) = event match {
    case ItemAdded(_, itemId, quantity) => state.updateItem(itemId, quantity)
  }
}
