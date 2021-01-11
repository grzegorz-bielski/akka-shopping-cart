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
  private val projectionTag = "testProjectionTag"
  private val eventSourcedTestKit =
    EventSourcedBehaviorTestKit[ShoppingCart.Command, ShoppingCart.Event, ShoppingCart.State](
      system,
      ShoppingCart(cartId, projectionTag))

  override protected def beforeEach() = {
    super.beforeEach()
    eventSourcedTestKit.clear()
  }

  "The Shopping Cart" should {
    "add item" in {
      val result =
        eventSourcedTestKit.runCommand(ShoppingCart.AddItem("abcd", 42, _))

      result.reply shouldEqual StatusReply.Success(
        ShoppingCart.Summary(Map("abcd" -> 42), isCheckedOut = false))
      result.event shouldEqual ShoppingCart.ItemAdded(cartId, "abcd", 42)
    }

    "checkout" in {
      eventSourcedTestKit
        .runCommand(ShoppingCart.AddItem("foo", 42, _))
        .reply
        .isSuccess shouldBe true

      val res =
        eventSourcedTestKit.runCommand(ShoppingCart.Checkout(_))
      res.reply shouldEqual StatusReply.Success(
        ShoppingCart.Summary(Map("foo" -> 42), isCheckedOut = true))

      res.event.asInstanceOf[ShoppingCart.CheckedOut].cartId shouldBe cartId

      eventSourcedTestKit
        .runCommand(ShoppingCart.AddItem("bar", 13, _))
        .reply
        .isSuccess shouldBe false
    }

    "reject already added item" in {
      eventSourcedTestKit
        .runCommand(ShoppingCart.AddItem("abcd", 42, _))
        .reply
        .isSuccess shouldBe true

      eventSourcedTestKit
        .runCommand(ShoppingCart.AddItem("abcd", 44, _))
        .reply
        .isSuccess shouldBe false
    }

    "get" in {

      eventSourcedTestKit
        .runCommand(ShoppingCart.AddItem("foo", 42, _))
        .reply
        .isSuccess shouldBe true

      eventSourcedTestKit.runCommand(ShoppingCart.Get(_)).reply shouldEqual ShoppingCart.Summary(
        Map("foo" -> 42),
        isCheckedOut = false)
    }
  }
}
