package shopping.cart

import akka.actor.typed.ActorSystem
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import akka.grpc.scaladsl.ServiceHandler
import akka.grpc.scaladsl.ServerReflection
import akka.http.scaladsl.Http
import scala.util.Success
import scala.util.Failure

object SoppingCartServer {
  def start(
      interface: String,
      port: Int,
      system: ActorSystem[_],
      grpcService: proto.ShoppingCartService): Unit = {
    implicit val sys: ActorSystem[_] = system
    implicit val ec: ExecutionContext = system.executionContext

    lazy val service =
      ServiceHandler.concatOrNotFound(
        proto.ShoppingCartServiceHandler.partial(grpcService),
        ServerReflection.partial(List(proto.ShoppingCartService)))

    Http()
      .newServerAt(interface, port)
      .bind(service)
      .map(_.addToCoordinatedShutdown(3.seconds))
      .onComplete {
        case Success(binding) =>
          val address = binding.localAddress
          system.log.info(
            "Shopping online at gRPC server {}:{}",
            address.getHostString,
            address.getPort)

        case Failure(exception) =>
          system.log.error("Failed to bind gRPC endpoint, terminating system", exception)
          system.terminate()
      }

  }
}
