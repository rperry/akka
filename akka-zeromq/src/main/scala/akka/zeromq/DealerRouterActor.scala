/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.zeromq

import akka.actor.Actor
import akka.actor.Actor._
import com.google.protobuf.Message
import java.util.UUID
import org.zeromq.ZMQ.{ Socket, Poller }
import org.zeromq.{ ZMQ ⇒ ZeroMQ }

private[zeromq] class DealerActor(params: SocketParameters) extends DealerRouterActor(ZeroMQ.XREQ, params)
private[zeromq] class RouterActor(params: SocketParameters) extends DealerRouterActor(ZeroMQ.XREP, params)

private[zeromq] abstract class DealerRouterActor(socketType: Int, params: SocketParameters) extends AbstractSocketActor(socketType, params) {
  private var inprocClientSocket: Socket = _
  private var inprocServerSocket: Socket = _
  private var poller: Poller = _
  override def receive: Receive = {
    case Start ⇒ self.supervisor.foreach { sup ⇒
      bindOrConnectRemoteSocket
      /* Create and bind/connect inproc sockets */
      inprocServerSocket = (sup ? SocketRequest(ZeroMQ.XREQ)).mapTo[Socket].get
      inprocServerSocket.bind("inproc://" + inprocServerSocketAddress)
      inprocClientSocket = (sup ? SocketRequest(ZeroMQ.XREQ)).mapTo[Socket].get
      inprocClientSocket.connect("inproc://" + inprocServerSocketAddress)
      /* Create poller */
      poller = (sup ? PollerRequest).mapTo[Poller].get
      poller.register(remoteSocket, Poller.POLLIN)
      poller.register(inprocServerSocket, Poller.POLLIN)
      receiveMessages
      self.reply(Ok)
    }
    case message: ZMQMessage ⇒ {
      send(inprocClientSocket, message.frames)
    }
    case message: Message ⇒ {
      inprocClientSocket.send(message.toByteArray, 0)
    }
  }
  private def receiveMessages = spawn {
    val (frontendIndex, inprocServerSocketIndex) = (0, 1)
    while (self.isRunning) {
      poller.poll(pollTimeoutMsec)
      if (poller.pollin(frontendIndex)) {
        receiveFrames(remoteSocket) match {
          case frames if (frames.length > 0) ⇒ params.listener.foreach { listener ⇒
            listener ! params.deserializer(frames)
          }
        }
      }
      if (poller.pollin(inprocServerSocketIndex)) {
        receiveFrames(inprocServerSocket) match {
          case frames if (frames.length > 0) ⇒ send(remoteSocket, frames)
        }
      }
    }
  }
  private lazy val pollTimeoutMsec = 100
  private lazy val inprocServerSocketAddress = UUID.randomUUID
}
