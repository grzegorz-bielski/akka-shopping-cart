package shopping.order

import akka.grpc.scaladsl.ServiceHandler
import akka.grpc.scaladsl.ServerReflection
import akka.http.scaladsl.Http
import akka.actor.typed.ActorSystem
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

object ShoppingOrderServer {
  def start(
      interface: String,
      port: Int,
      system: ActorSystem[_],
      grpcService: proto.ShoppingOrderService) = {
    implicit val sys = system
    implicit val ec = system.executionContext

    lazy val service = ServiceHandler.concatOrNotFound(
      proto.ShoppingOrderServiceHandler.partial(grpcService),
      ServerReflection.partial(List(proto.ShoppingOrderService)))

    Http()
      .newServerAt(interface, port)
      .bind(service)
      .map(_.addToCoordinatedShutdown(3.seconds))
      .onComplete {
        case Failure(exception) =>
          system.log.error("Failed to bind gRPC endpoint", exception)
          system.terminate()
        case Success(binding) =>
          val address = binding.localAddress
          system.log.info(
            "Shopping order at gRPC server {}:{}",
            address.getHostString,
            address.getPort)
      }
  }
}
