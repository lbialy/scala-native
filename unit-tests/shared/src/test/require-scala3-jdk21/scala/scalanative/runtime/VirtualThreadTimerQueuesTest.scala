package scala.scalanative.runtime

import org.junit.Assert._
import org.junit._

import scala.scalanative.junit.utils.AssumesHelper

/** The default scheduler must not start more timer threads than the JDK.
 *
 *  The JDK uses `max(highestOneBit(ncpus / 4), 1)` executors for delayed tasks
 *  (`jdk.virtualThreadScheduler.timerQueues` overrides it). Each one starts its
 *  thread lazily on its first `schedule()`, which for a virtual thread runs on
 *  a carrier, so every extra executor costs carrier time when virtual threads
 *  first sleep. Using `ncpus * 4` (16 on 4 CPUs) stalled all carriers for 10-25
 *  ms at once.
 */
object VirtualThreadTimerQueuesTest {
  @BeforeClass def checkRuntime(): Unit =
    AssumesHelper.assumeSupportsVirtualThreads()
}

class VirtualThreadTimerQueuesTest {
  private val Sleepers = 64
  private val SleepsPerThread = 4

  private def expectedQueueCount: Int =
    Option(System.getProperty("jdk.virtualThreadScheduler.timerQueues"))
      .map(Integer.parseInt)
      .getOrElse {
        val ncpus = Runtime.getRuntime().availableProcessors()
        Integer.highestOneBit(ncpus / 4).max(1)
      }

  private def timerThreads(): Seq[Thread] = {
    var root = Thread.currentThread().getThreadGroup()
    while (root.getParent() != null) root = root.getParent()
    val threads = new scala.Array[Thread](root.activeCount() + 64)
    val n = root.enumerate(threads, true)
    threads.take(n).toSeq.filter(_.getName() == "VirtualThread-unparker")
  }

  @Test def startsNoMoreTimerThreadsThanTheJDK(): Unit = {
    val sleepers = scala.Array.fill(Sleepers) {
      Thread.ofVirtual().start { () =>
        for (_ <- 1 to SleepsPerThread) Thread.sleep(1)
      }
    }
    sleepers.foreach(_.join())

    val count = timerThreads().size
    assertTrue(
      s"$count timer threads started, expected at most $expectedQueueCount",
      count <= expectedQueueCount
    )
  }
}
