/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.zeromq

import akka.actor.Actor
import akka.zeromq.test.Specification
import java.util.Arrays

class PubSubConnectionSpec extends Specification {
  private var messages = List[ZMQMessage]()
  "Pub-sub connection" should {
    "send / receive messages" in {
      val (publisher, subscriber) = (createPublisher, createSubscriber)
      publisher ! ZMQMessage(payload)
      waitUntil(messages.length == 1)
      messages.length must equal(1)
      Arrays.equals(messages.head.frames.head.payload, payload)
    }
    def createPublisher = {
      ZMQ.createPublisher(context, new SocketParameters(endpoint, Bind))
    }
    def createSubscriber = {
      ZMQ.createSubscriber(context, new SocketParameters(endpoint, Connect, Some(listener)))
    }
    lazy val listener = Actor.actorOf(new ListenerActor).start
    lazy val context = ZMQ.createContext
    lazy val endpoint = "tcp://127.0.0.1:" + endpointPort
    lazy val payload = "hello".getBytes
    lazy val endpointPort = randomPort
  }
  class ListenerActor extends Actor {
    def receive: Receive = {
      case message: ZMQMessage ⇒ {
        messages = message :: messages
      }
    }
  }
}
