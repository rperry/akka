/**
 * Copyright (C) 2009-2011 Scalable Solutions AB <http://scalablesolutions.se>
 */

package akka.actor

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.scalatest.BeforeAndAfterEach

import akka.util.ByteString
import akka.util.cps._
import akka.dispatch.Future
import scala.util.continuations._
import akka.testkit._

object IOActorSpec {

  class SimpleEchoServer(host: String, port: Int, ioManager: ActorRef, started: TestLatch) extends Actor {

    IO listen (ioManager, host, port)

    started.open

    val state = IO.IterateeRef.Map[IO.Handle]()

    def receive = {

      case IO.NewClient(server) ⇒
        val socket = server.accept()
        state(socket) flatMap { _ ⇒
          IO repeat {
            IO.takeAny map { bytes ⇒
              socket write bytes
            }
          }
        }

      case IO.Read(socket, bytes) ⇒
        state(socket)(IO Chunk bytes)

      case IO.Closed(socket, cause) ⇒
        state -= socket

    }

  }

  class SimpleEchoClient(host: String, port: Int, ioManager: ActorRef) extends Actor {

    val socket = IO connect (ioManager, host, port)

    val state = IO.IterateeRef()

    def receive = {

      case bytes: ByteString ⇒
        val source = channel
        socket write bytes
        for {
          _ ← state
          bytes ← IO take bytes.length
        } yield source tryTell bytes

      case IO.Read(socket, bytes) ⇒
        state(IO Chunk bytes)

      case IO.Connected(socket) ⇒

      case IO.Closed(socket, cause) ⇒
        self.stop()

    }
  }

  sealed trait KVCommand {
    def bytes: ByteString
  }

  case class KVSet(key: String, value: String) extends KVCommand {
    val bytes = ByteString("SET " + key + " " + value.length + "\r\n" + value + "\r\n")
  }

  case class KVGet(key: String) extends KVCommand {
    val bytes = ByteString("GET " + key + "\r\n")
  }

  case object KVGetAll extends KVCommand {
    val bytes = ByteString("GETALL\r\n")
  }

  // Basic Redis-style protocol
  class KVStore(host: String, port: Int, ioManager: ActorRef, started: TestLatch) extends Actor {

    val state = IO.IterateeRef.Map[IO.Handle]()

    var kvs: Map[String, String] = Map.empty

    IO listen (ioManager, host, port)

    started.open

    val EOL = ByteString("\r\n")

    def receive = {

      case IO.NewClient(server) ⇒
        val socket = server.accept()
        state(socket) flatMap { _ ⇒
          IO repeat {
            IO takeUntil EOL map (_.utf8String split ' ') flatMap {

              case Array("SET", key, length) ⇒
                for {
                  value ← IO take length.toInt
                  _ ← IO takeUntil EOL
                } yield {
                  kvs += (key -> value.utf8String)
                  ByteString("+OK\r\n")
                }

              case Array("GET", key) ⇒
                IO Iteratee {
                  kvs get key map { value ⇒
                    ByteString("$" + value.length + "\r\n" + value + "\r\n")
                  } getOrElse ByteString("$-1\r\n")
                }

              case Array("GETALL") ⇒
                IO Iteratee {
                  (ByteString("*" + (kvs.size * 2) + "\r\n") /: kvs) {
                    case (result, (k, v)) ⇒
                      val kBytes = ByteString(k)
                      val vBytes = ByteString(v)
                      result ++
                        ByteString("$" + kBytes.length) ++ EOL ++
                        kBytes ++ EOL ++
                        ByteString("$" + vBytes.length) ++ EOL ++
                        vBytes ++ EOL
                  }
                }

            } map (socket write)
          }
        }

      case IO.Read(socket, bytes) ⇒
        state(socket)(IO Chunk bytes)

      case IO.Closed(socket, cause) ⇒
        state -= socket

    }

  }

  class KVClient(host: String, port: Int, ioManager: ActorRef) extends Actor {

    val socket = IO connect (ioManager, host, port)

    val state = IO.IterateeRef()

    val EOL = ByteString("\r\n")

    def receive = {
      case cmd: KVCommand ⇒
        val source = channel
        socket write cmd.bytes
        for {
          _ ← state
          result ← readResult
        } yield result.fold(err ⇒ source sendException (new RuntimeException(err)), source tryTell)

      case IO.Read(socket, bytes) ⇒
        state(IO Chunk bytes)

      case IO.Connected(socket) ⇒

      case IO.Closed(socket, cause) ⇒
        self.stop()

    }

    def readResult: IO.Iteratee[Either[String, Any]] = {
      IO take 1 map (_.utf8String) flatMap {
        case "+" ⇒ IO takeUntil EOL map (msg ⇒ Right(msg.utf8String))
        case "-" ⇒ IO takeUntil EOL map (err ⇒ Left(err.utf8String))
        case "$" ⇒
          IO takeUntil EOL map (_.utf8String.toInt) flatMap {
            case -1 ⇒ IO Iteratee Right(None)
            case length ⇒
              for {
                value ← IO take length
                _ ← IO takeUntil EOL
              } yield Right(Some(value.utf8String))
          }
        case "*" ⇒
          IO takeUntil EOL map (_.utf8String.toInt) flatMap {
            case -1 ⇒ IO Iteratee Right(None)
            case length ⇒
              IO.takeList(length)(readResult) map { list ⇒
                ((Right(Map()): Either[String, Map[String, String]]) /: list.grouped(2)) {
                  case (Right(m), List(Right(Some(k: String)), Right(Some(v: String)))) ⇒ Right(m + (k -> v))
                  case (Right(_), _) ⇒ Left("Unexpected Response")
                  case (left, _) ⇒ left
                }
              }
          }
        case _ ⇒ IO Iteratee Left("Unexpected Response")
      }
    }
  }
}

class IOActorSpec extends WordSpec with MustMatchers with BeforeAndAfterEach {
  import IOActorSpec._

  "an IO Actor" must {
    "run echo server" in {
      val started = TestLatch(1)
      val ioManager = Actor.actorOf(new IOManager(2)) // teeny tiny buffer
      val server = Actor.actorOf(new SimpleEchoServer("localhost", 8064, ioManager, started))
      started.await
      val client = Actor.actorOf(new SimpleEchoClient("localhost", 8064, ioManager))
      val f1 = client ? ByteString("Hello World!1")
      val f2 = client ? ByteString("Hello World!2")
      val f3 = client ? ByteString("Hello World!3")
      f1.get must equal(ByteString("Hello World!1"))
      f2.get must equal(ByteString("Hello World!2"))
      f3.get must equal(ByteString("Hello World!3"))
      client.stop
      server.stop
      ioManager.stop
    }

    "run echo server under high load" in {
      val started = TestLatch(1)
      val ioManager = Actor.actorOf(new IOManager())
      val server = Actor.actorOf(new SimpleEchoServer("localhost", 8065, ioManager, started))
      started.await
      val client = Actor.actorOf(new SimpleEchoClient("localhost", 8065, ioManager))
      val list = List.range(0, 1000)
      val f = Future.traverse(list)(i ⇒ client ? ByteString(i.toString))
      assert(f.get.size === 1000)
      client.stop
      server.stop
      ioManager.stop
    }

    "run echo server under high load with small buffer" in {
      val started = TestLatch(1)
      val ioManager = Actor.actorOf(new IOManager(2))
      val server = Actor.actorOf(new SimpleEchoServer("localhost", 8066, ioManager, started))
      started.await
      val client = Actor.actorOf(new SimpleEchoClient("localhost", 8066, ioManager))
      val list = List.range(0, 1000)
      val f = Future.traverse(list)(i ⇒ client ? ByteString(i.toString))
      assert(f.get.size === 1000)
      client.stop
      server.stop
      ioManager.stop
    }

    "run key-value store" in {
      val started = TestLatch(1)
      val ioManager = Actor.actorOf(new IOManager(2)) // teeny tiny buffer
      val server = Actor.actorOf(new KVStore("localhost", 8067, ioManager, started))
      started.await
      val client1 = Actor.actorOf(new KVClient("localhost", 8067, ioManager))
      val client2 = Actor.actorOf(new KVClient("localhost", 8067, ioManager))
      val f1 = client1 ? KVSet("hello", "World")
      val f2 = client1 ? KVSet("test", "No one will read me")
      val f3 = client1 ? KVGet("hello")
      f2.await
      val f4 = client2 ? KVSet("test", "I'm a test!")
      f4.await
      val f5 = client1 ? KVGet("test")
      val f6 = client2 ? KVGetAll
      f1.get must equal("OK")
      f2.get must equal("OK")
      f3.get must equal(Some("World"))
      f4.get must equal("OK")
      f5.get must equal(Some("I'm a test!"))
      f6.get must equal(Map("hello" -> "World", "test" -> "I'm a test!"))
      client1.stop
      client2.stop
      server.stop
      ioManager.stop
    }
  }

}
