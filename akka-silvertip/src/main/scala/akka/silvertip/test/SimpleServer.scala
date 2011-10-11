package akka.silvertip.test

import akka.event.EventHandler
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import scala.collection.JavaConversions._
import silvertip.{Connection, MessageParser, Events}

object SimpleServer extends App with SimpleServerClientConfig {
  EventHandler.info(this, "Waiting for connection...")
  val connection = Connection.accept(new InetSocketAddress(serverPort), new SimpleMessageParser, 
    new Connection.Callback[String]() {
      def messages(connection: Connection[String], messages: java.util.Iterator[String]) = {
        messages.foreach { message => 
          EventHandler.info(this, "Message: %s".format(message))
        }
      }
      def idle(connection: Connection[String]) = Unit
      def closed(connection: Connection[String]) = Unit
      def garbledMessage(message: String, data: Array[Byte]) = Unit
    }
  )
  EventHandler.info(this, "Connection accepted")
  val events = Events.open(timeoutIntervalMsec)
  events.register(connection)
  events.dispatch
  EventHandler.info(this, "Connection closed")
  lazy val timeoutIntervalMsec = 100
}

class SimpleMessageParser extends MessageParser[String] {
  def parse(byteBuffer: ByteBuffer): String = {
    val buffer = new StringBuffer
    while (byteBuffer.hasRemaining)
      buffer.append(byteBuffer.get.asInstanceOf[Char])
    buffer.toString
  }
}
