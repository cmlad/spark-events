import java.io.{InputStream, PrintStream}
import java.net.ServerSocket

class EventsEmitterServer extends Thread {
  override def run() {
    println("Events emitter server thread started")

    val stream: InputStream = getClass.getResourceAsStream("/events.jsonl")
    val lines = scala.io.Source.fromInputStream(stream).getLines

    val server = new ServerSocket(9999)

    while (true) {
      val s = server.accept()
      val out = new PrintStream(s.getOutputStream)

      lines.foreach { line =>
        out.println(line.trim)
        Thread.sleep(1000)
        out.flush()
      }

      s.close()
    }
  }
}
