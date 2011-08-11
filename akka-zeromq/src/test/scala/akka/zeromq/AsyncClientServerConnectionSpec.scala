/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.zeromq

import akka.actor.{ Actor, ActorRef }
import org.scalatest.{ BeforeAndAfter }
import akka.zeromq.test.Specification

class AsyncClientServerConnectionSpec extends Specification with BeforeAndAfter {
  "Asynchronous client-server connection" should {
    var endpoint: String = ""
    before {
      endpoint = "tcp://127.0.0.1:" + randomPort
    }
    val (numRequests, numResponses) = (3, 2)
    "send N requests, receive 0 responses" in {
      val (server, client) = (createServer, createClient)
      (1 to numRequests).foreach { id ⇒ client ! ZMQMessage("request:%s".format(id).getBytes) }
      waitUntil(serverMessages.length == numRequests)
      serverMessages.length must equal(numRequests)
    }
    "send N requests, receive M responses" in {
      val (client, server) = (createClient, createServer)
      (1 to numRequests).foreach { id ⇒ client ! ZMQMessage("request:%s".format(id).getBytes) }
      (1 to numResponses).foreach { id ⇒ server ! ZMQMessage("response:%s".format(id).getBytes) }
      waitUntil(clientMessages.length == numResponses && serverMessages.length == numRequests)
      clientMessages.length must equal(numResponses)
      serverMessages.length must equal(numRequests)
    }
    "send 0 requests, receive N responses" in {
      val (client, server) = (createClient, createServer)
      (1 to numResponses).foreach { id ⇒ server ! ZMQMessage("response:%s".format(id).getBytes) }
      waitUntil(clientMessages.length == numResponses)
      clientMessages.length must equal(numResponses)
    }
    def createClient = {
      ZMQ.createDealer(context, new SocketParameters(endpoint, Connect, Some(clientListener)))
    }
    def createServer = {
      ZMQ.createDealer(context, new SocketParameters(endpoint, Bind, Some(serverListener)))
    }
    def serverMessages = {
      messages(serverListener)
    }
    def clientMessages = {
      messages(clientListener)
    }
    def messages(listener: ActorRef) = {
      (listener ? QueryMessages).mapTo[List[ZMQMessage]].get
    }
    lazy val clientListener = Actor.actorOf(new ListenerActor).start
    lazy val serverListener = Actor.actorOf(new ListenerActor).start
    lazy val context = ZMQ.createContext
  }
  class ListenerActor extends Actor {
    private var messages = List[ZMQMessage]()
    def receive: Receive = {
      case message: ZMQMessage ⇒ {
        messages = message :: messages
      }
      case QueryMessages ⇒ {
        self.reply(messages)
      }
    }
  }
  case object QueryMessages
}
