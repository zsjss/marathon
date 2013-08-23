package mesosphere.marathon.executor

import java.util.logging.Logger
import org.apache.mesos._
import org.apache.mesos.Protos._
import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors
import mesosphere.marathon.api.v1.AppDefinition
import java.io.File
import scala.util.matching.Regex


class MarathonExecutor(val terminateTimeout: Int = 10000) extends Executor {
  val tasks = new mutable.HashMap[TaskID, UNIXProc]
             with mutable.SynchronizedMap[TaskID, UNIXProc]
  val killed = new mutable.HashSet[TaskID]
              with mutable.SynchronizedSet[TaskID]
  private[this] val log = Logger.getLogger(getClass.getName)
  private[this] val pool = Executors.newCachedThreadPool()
  private[this] val ctx = ExecutionContext.fromExecutorService(pool)
  var slaveInfo: SlaveInfo = null

  private[this] def forkAndLogErrors[T](f: => T) = future({
    try f catch {
      case e: Throwable => {
        log.severe(f"in ${Thread.currentThread.getId}, error: ${e}")
        throw e
      }
    }
  })(ctx)

  def registered(driver: ExecutorDriver,
                 executorInfo: ExecutorInfo,
                 frameworkInfo: FrameworkInfo,
                 slaveInfo: SlaveInfo) {

    this.slaveInfo = slaveInfo
    log.info(f"executor on slave: ${slaveInfo.getHostname}")
  }

  def reregistered(driver: ExecutorDriver, slaveInfo: SlaveInfo) {
    log.info(f"executor on slave: ${slaveInfo.getHostname}")
  }

  def disconnected(driver: ExecutorDriver) {
    if (this.slaveInfo == null) {
      log.warning("lost connection to null slave?")
    } else {
      log.warning(f"lost connection to ${this.slaveInfo.getHostname}")
    }
  }

  def launchTask(driver: ExecutorDriver, taskInfo: TaskInfo) {
    val app: AppDefinition = new AppDefinition()
    app.mergeFromProto(taskInfo.getData.toByteArray)
    val tid: TaskID = taskInfo.getTaskId
    log.info(f"${tid.getValue} -- ${app.cmd}")
    forkAndLogErrors { run(driver, app, tid) }
  }

  def killTask(driver: ExecutorDriver, tid: TaskID) {
    log.info(f"${tid.getValue} marked for destruction")
    forkAndLogErrors { gracefulTermination(tid) }
  }

  def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]) {}

  def shutdown(driver: ExecutorDriver) {
    for (tid <- tasks.keys) forkAndLogErrors { killTask(driver, tid) }
  }

  def error(driver: ExecutorDriver, message: String) = log.warning(message)

  /**
   * Terminate a process and its subprocesses gracefully, first signalling the
   * parent and then destroying the process group.
   *
   * NB: We assume every process we run has set its PGID to its PID. Therefore,
   * it is safe to kill everything with this process's PGID even after it dies,
   * since the loggers are still running and they are holding the PGID.
   * @param tid
   */
  def gracefulTermination(tid: TaskID, millis: Int = terminateTimeout) {
    tasks.get(tid) match {
      case None => log.warning(f"TID ${tid.getValue} No process found!")
      case Some(proc) => {
        val pidForTID = f"PID ${proc.pid} TID ${tid.getValue}"
        log.info(f"$pidForTID Found process for task")
        killed += tid

        proc.kill("TERM").run() // Send only to *parent*; let it clean up
        Thread.sleep(millis)

        if (tasks.contains(tid)) {
          log.warning(f"$pidForTID No exit status collected after $millis ms")
        }

        if (scanForLingeringProcesses(proc.pid).run() == 0) {
          log.warning(f"$pidForTID Lingering processes in process group")
          proc.kill("KILL", asSessionLeader = true).run()
        }
      }
    }
  }

  def run(driver: ExecutorDriver, app: AppDefinition, tid: TaskID) {
    val procBuilder: UNIXBuilder = commandToRun(app)
    val proc: UNIXProc = try procBuilder.proc catch {
      case e: Throwable => {
        log.severe(f"Error starting ${tid.getValue} $e")
        sendStatus(driver, tid, TaskState.TASK_FAILED)
        throw e
      }
    }

    sendStatus(driver, tid, TaskState.TASK_RUNNING)
    log.info(
      f"PID ${proc.pid} TID ${tid.getValue} ARGV ${procBuilder.argvString}"
    )
    tasks(tid) = proc
    proc.run() // NB: Blocking call

    val state = (proc.exitCode == 0, killed.contains(tid)) match {
      case (true,  _)     => TaskState.TASK_FINISHED
      case (false, true)  => TaskState.TASK_KILLED
      case (false, false) => TaskState.TASK_FAILED
    }

    sendStatus(driver, tid, state)
    if (scanForLingeringProcesses(proc.pid).run() == 0) {
      proc.kill("TERM", asSessionLeader = true).run()
    }

    tasks  -= tid
    killed -= tid
  }

  case class BadSpecialCmd(name: String)
     extends Exception(f"Bad special command: $name")

  case class MissingSpecialCmd(name: String, pluginsDir: String)
    extends Exception(f"Failed to find plugin $name (in $pluginsDir)")

  /**
   *  This method wraps commands in logging via logger and selects a special
   *  code path if commands begin with the string '//'. The Syslog tag is
   *  set to the name of the special wrapper, prepended with 'marathon', or to
   *  'marathon//sh' if the default strategy of passing to 'sh' is used.
   *
   * @param app application to run
   * @return    a process builder for UNIX
   */
  def commandToRun(app: AppDefinition): UNIXBuilder = {
    val cmd = app.cmd.trim
    val (cmdWord, rest) = (cmd.takeWhile(_ != ' '), cmd.dropWhile(_ != ' '))
    val tail = rest.trim
    val SpecialCmd = "^//([a-zA-Z0-9._:+-]+)$".r            // Has to look nice
    val IllegalCmd = "^//.+$".r         // Looks like special command but isn't
    val byShell = (cmd, "marathon//sh")
    val (toRun, tag): (String, String) = cmdWord match {
      case SpecialCmd("sh") => byShell  // You can force use of shell with //sh
      case SpecialCmd(c)    => (f"exec ${plugin(c)} $tail", f"marathon//$c")
      case IllegalCmd(text) => throw BadSpecialCmd(text)
      case _                => byShell
    }

    /*  Layers of wrapping:

     ** Wrapped with 'setsid' to get a fresh session, for process tracking.

     ** Wrapped with Bash logging utility for syslog

     ** Wrapped with 'exec' (the first time) to ensure the PID being logged is
        the PID of our task.

     ** Wrapped with 'sh -c' so that using variables (like $PORT and $PWD) and
        other shell features works as expected.

     ** Wrapped with 'exec' the second time if it is a special command plugin,
        so that signals are received by the special command and not by the
        shell. This allows the special command plugin to take full control of
        process cleanup. One effect of prefixing the command with 'exec' is
        that any following '&&', '||' or ';' clauses are ignored -- the shell
        has already been replaced with the plugin.

        This is a confusing situation but is unavoidable if shell-like
        behaviour -- variables and all the rest -- is to be presented as the
        default. Far better would be to make the default an array of plain
        words, to be passed to exec, and make variables and the like in to
        special cases.

     */

    val toExec = Seq("sh", "-c", toRun)
    val argv: Seq[String] =
      Seq("setsid", MarathonExecutor.wrapper, "logged", tag, "exec") ++ toExec
    UNIXBuilder(argv, app.env, log)
  }

  def scanForLingeringProcesses(pgid: Int) = UNIXBuilder(
    Seq("ps", "-g", f"$pgid", "-o", "pid="), Map(), log,
    okayExit = Set(0, 1)
  )

  def plugin(name: String): String = {
    val f = new File(f"${MarathonExecutor.pluginsDir}/$name")
    if (! f.exists) throw MissingSpecialCmd(name, MarathonExecutor.pluginsDir)
    f.setExecutable(true)
    f.setReadable(true)
    f.getAbsolutePath
  }

  def sendStatus(driver: ExecutorDriver, tid: TaskID, state: TaskState) {
    val status = TaskStatus.newBuilder()
      .setTaskId(tid)
      .setState(state).build()
    driver.sendStatusUpdate(status)
  }
}

object MarathonExecutor {
  def main(args: Array[String]) {
    val executor = new MarathonExecutor()
    val executorDriver = new MesosExecutorDriver(executor)
    val status = executorDriver.run()
    status match {
      case Status.DRIVER_STOPPED => System.exit(0)
      case _ => System.exit(1)
    }
  }

  // TODO: Make this configurable.
  val marathonRoot = "/opt/marathon"
  val wrapper = f"$marathonRoot/bin/marathon-framework"
  val pluginsDir = f"$marathonRoot/plugins"

  val id = ExecutorID.newBuilder.setValue("marathon-standard")
  val command = CommandInfo.newBuilder.setValue(f"exec $wrapper executor")
  val info = ExecutorInfo.newBuilder
    .setCommand(command)
    .setSource("mesosphere.io")
    .setExecutorId(id).build()
}
