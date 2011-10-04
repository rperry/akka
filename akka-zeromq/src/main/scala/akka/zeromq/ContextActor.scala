/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.zeromq

import akka.actor.Actor
import org.zeromq.ZMQ.Context
import org.zeromq.{ ZMQ ⇒ ZeroMQ }

private[zeromq] class ContextActor extends Actor {
  private var zmqContext: Context = _
  override def receive: Receive = {
    case Start ⇒ {
      zmqContext = ZeroMQ.context(1)
      reply(Ok)
    }
    case SocketRequest(socketType) ⇒ {
      reply(zmqContext.socket(socketType))
    }
    case PollerRequest ⇒ {
      reply(zmqContext.poller)
    }
  }
}
