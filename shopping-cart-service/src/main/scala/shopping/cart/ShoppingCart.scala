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
import java.time.Instant
import akka.cluster.sharding.typed.scaladsl.EntityContext

object ShoppingCart {
  // state
  object State {
    val empty = State(items = Map.empty, checkoutDate = None)
  }

  final case class State(items: Map[String, Int], checkoutDate: Option[Instant])
      extends CborSerializable {
    def hasItem(itemId: String) = items.contains(itemId)
    def isEmpty = items.isEmpty
    def checkout(now: Instant) = copy(checkoutDate = Some(now))
    def isCheckedOut = checkoutDate.isDefined
    def updateItem(itemId: String, quantity: Int) = quantity match {
      case 0 => copy(items = items - itemId)
      case _ => copy(items = items + (itemId -> quantity))
    }
    def toSummary = Summary(items, isCheckedOut)
  }

  // init
  val EntityKey = EntityTypeKey[Command]("ShoppingCart")
  val tags = Vector.tabulate(5)(i => s"carts-$i")

  def init(system: ActorSystem[_]) = {
    val behaviorFactory = { (ctx: EntityContext[Command]) =>
      val tag = tags(math.abs(ctx.entityId.hashCode % tags.size))

      ShoppingCart(ctx.entityId, tag)
    }

    ClusterSharding(system).init(Entity(EntityKey)(behaviorFactory))
  }

  def apply(cartId: String, projectionTag: String) =
    EventSourcedBehavior
      .withEnforcedReplies[Command, Event, State](
        persistenceId = PersistenceId(EntityKey.name, cartId),
        emptyState = State.empty,
        commandHandler = Command.handle(cartId, _, _),
        eventHandler = Event.handle)
      .withTagger(_ => Set(projectionTag))
      .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 100, keepNSnapshots = 3))
      .onPersistFailure(SupervisorStrategy
        .restartWithBackoff(minBackoff = 200.millis, maxBackoff = 5.seconds, randomFactor = 0.1))

  // commands

  final case class Summary(items: Map[String, Int], isCheckedOut: Boolean) extends CborSerializable

  sealed trait Command extends CborSerializable
  final case class AddItem(itemId: String, quantity: Int, replyTo: ActorRef[StatusReply[Summary]])
      extends Command
  final case class Checkout(replyTo: ActorRef[StatusReply[Summary]]) extends Command
  final case class Get(replyTo: ActorRef[Summary]) extends Command

  object Command {
    def handle(cartId: String, state: State, command: Command): ReplyEffect[Event, State] =
      if (state.isCheckedOut)
        checkedOutShoppingCart(cartId, state, command)
      else
        openShoppingCart(cartId, state, command)

    private def openShoppingCart(
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
            Effect
              .persist(ItemAdded(cartId, itemId, quantity))
              .thenReply(replyTo)(s => StatusReply.Success(s.toSummary))

        case Get(replyTo) =>
          Effect.reply(replyTo)(state.toSummary)

        case Checkout(replyTo) =>
          if (state.isEmpty)
            Effect.reply(replyTo)(StatusReply.Error("Cannot checkout an empty shopping cart"))
          else
            Effect
              .persist(CheckedOut(cartId, Instant.now))
              .thenReply(replyTo)(s => StatusReply.Success(s.toSummary))
      }

    private def checkedOutShoppingCart(
        cartId: String,
        state: State,
        command: Command): ReplyEffect[Event, State] =
      command match {
        case Get(replyTo) =>
          Effect.reply(replyTo)(state.toSummary)

        case cmd @ AddItem(_, _, _) =>
          Effect.reply(cmd.replyTo)(
            StatusReply.Error("Can't add an item to an already checked out shopping cart"))
        case cmd @ Checkout(_) =>
          Effect.reply(cmd.replyTo)(
            StatusReply.Error("Can't checkout already checked out shopping cart"))
      }
  }

  // events

  sealed trait Event extends CborSerializable {
    def cartId: String
  }
  final case class ItemAdded(cartId: String, itemId: String, quantity: Int) extends Event
  final case class CheckedOut(cartId: String, eventTime: Instant) extends Event

  object Event {
    def handle(state: State, event: Event): State = event match {
      case ItemAdded(_, itemId, quantity) =>
        state.updateItem(itemId, quantity)
      case CheckedOut(_, eventTime) =>
        state.checkout(eventTime)
    }
  }
}
