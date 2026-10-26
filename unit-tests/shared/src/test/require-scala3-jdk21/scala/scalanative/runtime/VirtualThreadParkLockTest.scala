package scala.scalanative.runtime

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.LockSupport
import java.util.concurrent.{
  CountDownLatch, ExecutorService, Executors, ScheduledExecutorService,
  ThreadFactory, TimeUnit
}

import org.junit.Assert._
import org.junit._

import scala.scalanative.junit.utils.AssumesHelper

/** Carrier compensation must not run while PosixThread.park holds its lock.
 *
 *  A virtual thread waiting on an inflated monitor parks its carrier. The
 *  compiler wraps the wait in Blocker.begin, which runs the scheduler's
 *  compensation; that code can reach unpark() of the parking carrier (seen
 *  through a GC callback). If it runs under park's lock, unpark blocks on that
 *  lock forever. The hook below unparks the carrier from compensation started
 *  by park; the monitor must still be entered, and the test must fail rather
 *  than hang when it is not.
 */
object VirtualThreadParkLockTest {
  @BeforeClass def checkRuntime(): Unit =
    AssumesHelper.assumeSupportsVirtualThreads()
}

class VirtualThreadParkLockTest {
  private class UnparkingScheduler(
      pool: ExecutorService,
      timers: ScheduledExecutorService
  ) extends VirtualThreadScheduler {
    val armed = new AtomicBoolean(true)
    @volatile var hookRan = false

    def execute(task: Runnable): Unit = pool.execute(task)
    def schedule(
        task: Runnable,
        delay: Long,
        unit: TimeUnit
    ): VirtualThreadScheduler.Cancellable = {
      val f = timers.schedule(task, delay, unit)
      new VirtualThreadScheduler.Cancellable {
        def cancel(): Boolean = f.cancel(false)
        def isDone(): Boolean = f.isDone()
      }
    }

    // Walking the stack can itself make blocking calls, which re-enter this
    // hook through Blocker.begin.
    private val inHook = new ThreadLocal[java.lang.Boolean]

    private def calledFromPark(): Boolean =
      if (inHook.get() eq java.lang.Boolean.TRUE) false
      else {
        inHook.set(java.lang.Boolean.TRUE)
        try
          new Exception().getStackTrace().exists { f =>
            f.getClassName() == "java.lang.impl.PosixThread" &&
            f.getMethodName() == "park"
          }
        finally inHook.set(java.lang.Boolean.FALSE)
      }

    override def beginCarrierCompensatedBlock(carrier: Thread): Boolean = {
      if (armed.get() && calledFromPark() && armed.compareAndSet(true, false)) {
        hookRan = true
        LockSupport.unpark(carrier)
      }
      false
    }
  }

  @Test def compensationMayUnparkTheParkingCarrier(): Unit = {
    val daemons: ThreadFactory = { r =>
      val t = new Thread(r)
      t.setDaemon(true) // a deadlocked carrier must not keep the process alive
      t
    }
    val pool = Executors.newFixedThreadPool(2, daemons)
    val timers = Executors.newSingleThreadScheduledExecutor(daemons)
    val scheduler = new UnparkingScheduler(pool, timers)
    val previous = VirtualThread.defaultScheduler
    VirtualThread.setDefaultScheduler(scheduler)
    try {
      val lock = new Object
      val entered = new AtomicBoolean(false)
      val holding = new CountDownLatch(1)
      val release = new CountDownLatch(1)
      // Releasing the monitor unparks the waiter's carrier, which blocks for
      // good if that carrier is stuck in park, so the test thread must not
      // hold it.
      val holder = daemons.newThread { () =>
        lock.synchronized {
          lock.wait(
            1
          ) // inflate: waiters on an inflated monitor park the carrier
          holding.countDown()
          release.await()
        }
      }
      holder.start()
      holding.await()
      val vt =
        Thread.ofVirtual().start(() => lock.synchronized(entered.set(true)))
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
      while (!scheduler.hookRan && System.nanoTime() < deadline)
        Thread.sleep(1)
      release.countDown()
      vt.join(TimeUnit.SECONDS.toMillis(10))
      assertTrue("compensation hook never ran from park", scheduler.hookRan)
      assertTrue(
        "virtual thread never entered the monitor: carrier stuck in park",
        entered.get()
      )
    } finally {
      VirtualThread.setDefaultScheduler(previous)
      pool.shutdown()
      timers.shutdown()
    }
  }
}
