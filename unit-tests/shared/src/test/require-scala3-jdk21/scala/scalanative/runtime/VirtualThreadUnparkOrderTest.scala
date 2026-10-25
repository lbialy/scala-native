package scala.scalanative.runtime

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.LockSupport

import org.junit.Assert._
import org.junit._

import scala.scalanative.junit.utils.AssumesHelper

/** A virtual thread unparked by another virtual thread must not be queued
 *  behind work that waits for it.
 *
 *  On the JDK, `VirtualThread.unpark` called from a virtual thread switches to
 *  the carrier before submitting, so the woken thread lands on that carrier's
 *  local queue and runs as soon as the current task ends. Submitting to the
 *  shared queue instead puts it behind spinners that hold every carrier until
 *  it runs: a livelock (seen as Ox channel `done()` spinning on a stale select
 *  clause). Spinners give up after a deadline, so a regression fails instead of
 *  hanging.
 */
object VirtualThreadUnparkOrderTest {
  @BeforeClass def checkRuntime(): Unit =
    AssumesHelper.assumeSupportsVirtualThreads()
}

class VirtualThreadUnparkOrderTest {
  private val Iterations = 200
  private val Spinners = 64
  private val SpinDeadlineNanos = TimeUnit.SECONDS.toNanos(2)

  @Test def unparkedThreadRunsBeforeQueuedSpinners(): Unit = {
    for (iteration <- 1 to Iterations) {
      val ownerRan = new AtomicBoolean(false)
      val spinnerGaveUp = new AtomicBoolean(false)
      val owner = Thread.ofVirtual().start { () =>
        while (!ownerRan.get()) {
          LockSupport.park()
          ownerRan.set(true)
        }
      }
      while (owner.getState() != Thread.State.WAITING) Thread.onSpinWait()

      val waker = Thread.ofVirtual().start(() => LockSupport.unpark(owner))
      val spinners = scala.Array.fill(Spinners) {
        Thread.ofVirtual().start { () =>
          val deadline = System.nanoTime() + SpinDeadlineNanos
          while (!ownerRan.get()) {
            if (System.nanoTime() - deadline > 0) {
              spinnerGaveUp.set(true)
              ownerRan.set(true) // release the others; the test has failed
            } else Thread.onSpinWait()
          }
        }
      }
      waker.join()
      spinners.foreach(_.join())
      owner.join()
      assertFalse(
        s"iteration $iteration: unparked owner did not run while $Spinners spinners waited for it",
        spinnerGaveUp.get()
      )
    }
  }
}
