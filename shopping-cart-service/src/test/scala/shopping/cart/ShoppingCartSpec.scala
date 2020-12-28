package shopping.cart

import com.typesafe.config.ConfigFactory
import akka.persistence.testkit.scaladsl.EventSourcedBehaviorTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.BeforeAndAfterEach
import akka.pattern.StatusReply

object ShoppingCartSpec {
  val config = ConfigFactory
    .parseString("""
     akka.actor.serialization-bindings {
         "shopping.cart.CborSerializable" = jackson-cbor
     }
    """)
    .withFallback(EventSourcedBehaviorTestKit.config)
}

class ShoppingCartSpec
    extends ScalaTestWithActorTestKit(ShoppingCartSpec.config)
    with AnyWordSpecLike
    with BeforeAndAfterEach {

  private val cartId = "testCardId"
  private val eventSourcedTestKit =
    EventSourcedBehaviorTestKit[ShoppingCart.Command, ShoppingCart.Event, ShoppingCart.State](
      system,
      ShoppingCart(cartId))

  override protected def beforeEach() = {
    super.beforeEach()
    eventSourcedTestKit.clear()
  }

  "The Shopping Cart" should {
    "add item" in {
      val result =
        eventSourcedTestKit.runCommand(replyTo => ShoppingCart.AddItem("abcd", 42, replyTo))

      result.reply shouldEqual StatusReply.Success(ShoppingCart.Summary(Map("abcd" -> 42)))
      result.event shouldEqual ShoppingCart.ItemAdded(cartId, "abcd", 42)
    }

    "reject already added item" in {
      eventSourcedTestKit
        .runCommand(replyTo => ShoppingCart.AddItem("abcd", 42, replyTo))
        .reply
        .isSuccess shouldBe true

      eventSourcedTestKit
        .runCommand(replyTo => ShoppingCart.AddItem("abcd", 44, replyTo))
        .reply
        .isSuccess shouldBe false
    }
  }
}
