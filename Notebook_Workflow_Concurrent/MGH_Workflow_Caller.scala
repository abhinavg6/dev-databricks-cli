// Databricks notebook source
import java.util.concurrent.Executors
import scala.concurrent.{Future, ExecutionContext, Await}
import scala.concurrent.duration._
import com.databricks.WorkflowException

val ctx = dbutils.notebook.getContext()
// create a scala execution context from a Java thread pool
implicit val ec = ExecutionContext
  .fromExecutor(Executors.newFixedThreadPool(2)) // use a number according to parallel jobs, or use a unbounded newCachedThreadPool()

// define a general method to run a notebook, and do as-needed error handling
def runNotebook(nbName: String): String = {
  var status = "success"
  try {
    dbutils.notebook.run("./" + nbName, 60, Map("data1" -> "something", "data2" -> "something else"))
  } catch {
    // catch parent workflow exception and failure status
    case e: WorkflowException => {
      println("Error executing notebook " + nbName + " : " + e)
      status = "failure"
    }
  }
  status
}

val initTasksNBs = List("MGH_Test1", "MGH_Test2")
// Return parallel async tasks for initial notebooks
val initTasks: Seq[Future[String]] = for (nb <- initTasksNBs) yield Future {
  println("Executing initial notebook " + nb)
  dbutils.notebook.setContext(ctx)
  runNotebook(nb)
}
val initCombinedTask: Future[Seq[String]] = Future.sequence(initTasks)
// Wait until all parallel async tasks have completed or until specified timeout
val initStatuses: Seq[String] = Await.result(initCombinedTask, 60.seconds)

// Run subsequent notebook if all initial notebooks were a success
if(initStatuses.forall(x => x == "success")) {
  val subsequentNB = "MGH_Test3"
  println("Executing subsequent notebook " + subsequentNB)
  runNotebook(subsequentNB) // this will run in main thread, one could optionally run this with thread pool as well
}

// COMMAND ----------

