package akka.silvertip.test

import akka.actor.Actor
import akka.event.EventHandler
import akka.silvertip._

object SimpleClient extends App with SimpleServerClientConfig {
  Silvertip.createConnection(new ConnectionParameters(
    new MessageParserFactory[String] {
      def create = new SimpleMessageParser
    },
    Actor.actorOf(new Actor {
      def receive: Receive = { case message => 
        EventHandler.info(this, "Message: %s".format(message))
      }
    }).start, "localhost", serverPort
  ))
}
