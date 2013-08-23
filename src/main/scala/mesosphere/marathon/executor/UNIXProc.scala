package mesosphere.marathon.executor

import java.lang
import java.lang.reflect.Field
import java.util.logging.Logger
import scala.collection.JavaConverters._

case class UNIXProc(proc:     lang.Process,
                    log:      Logger = Logger.getLogger(getClass.getName),
                    okayExit: Set[Int] = Set(0)) {

  lazy val pid: Int = {
    // We use java.lang.Process instead of scala.sys.process.Process solely
    // because we can extract the PID through bad reflection tricks from the
    // former and not the latter. The PID is critical for sending signals during
    // kill escalation.
    try {
      val f: Field = proc.getClass().getDeclaredField("pid")
      f.setAccessible(true)
      f.get(proc).asInstanceOf[Int]
    } catch {
      case e: Throwable => {
        log.severe(f"error while reading PID: $e")
        throw e
      }
    }
  }

  def run() = exitCode

  lazy val exitCode: Int = runAndWait()

  private def runAndWait(): Int = {
    proc.waitFor()
    val exit = proc.exitValue()
    if (! okayExit.contains(exit)) log.warning(f"PID $pid EXIT $exit")
    exit
  }

  def kill(signal: String, asSessionLeader: Boolean = false): UNIXBuilder = {
    val argv = if (asSessionLeader) {
      Seq("kill", "-s", signal, "--", f"-$pid")
    } else {
      Seq("kill", "-s", signal, f"$pid")
    }
    UNIXBuilder(argv, Map(), log)
  }
}

object UNIXProc {
  def escapedCommand(strings: Seq[String]): Seq[String] =
    for (word <- strings) yield {
      if (word.contains(' ') || word.contains('"'))
        '\'' + word + '\''
      else
        word
    }
}

case class UNIXBuilder(argv: Seq[String],
                       envs: Map[String,String] = Map(),
                       log: Logger = Logger.getLogger(getClass.getName),
                       okayExit: Set[Int] = Set(0)) {

  private val builder: lang.ProcessBuilder = {
    // NB: Might need to drop inheritIO on JDK 6
    val pb = new lang.ProcessBuilder(argv:_*).inheritIO
    for ((k,v) <- envs) pb.environment().put(k,v)
    pb
  }

  val command: Seq[String] = builder.command().asScala

  val argvString: String = command.mkString(" ")

  lazy val proc: UNIXProc = start()

  private def start(): UNIXProc = {
    try {
      val p = UNIXProc(builder.start(), log, okayExit)
      log.severe(f"PID ${p.pid} ARGV $argvString")
      p
    } catch {
      case e: Throwable => {
        log.severe(f"EXCEPTION $argvString")
        throw e
      }
    }
  }

  lazy val exitCode: Int = proc.exitCode

  def run() = proc.run()
}
