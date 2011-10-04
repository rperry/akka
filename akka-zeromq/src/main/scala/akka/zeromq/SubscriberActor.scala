/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.zeromq

import akka.actor.Actor._
import org.zeromq.{ ZMQ ⇒ ZeroMQ }

private[zeromq] class SubscriberActor(params: SocketParameters) extends AbstractSocketActor(ZeroMQ.SUB, params) {
  override def receive: Receive = {
    case Start ⇒ {
      bindOrConnectRemoteSocket; remoteSocket.subscribe(Array.empty); receiveMessages
      reply(Ok)
    }
  }
  private def receiveMessages = spawn {
    while (self.isRunning) {
      receiveFrames(remoteSocket) match {
        case frames if (frames.length > 0) ⇒ params.listener.foreach { listener ⇒
          listener ! params.deserializer(frames)
        }
      }
    }
  }
}
