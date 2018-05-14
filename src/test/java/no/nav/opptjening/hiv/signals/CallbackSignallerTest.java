package no.nav.opptjening.hiv.signals;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class CallbackSignallerTest {
    @Test
    public void signal() throws InterruptedException {
        Signaller.CallbackSignaller signaller = new Signaller.CallbackSignaller();

        CountDownLatch callbacksCalled = new CountDownLatch(2);

        CallbackTask c1 = new CallbackTask(callbacksCalled);
        CallbackTask c2 = new CallbackTask(callbacksCalled);
        Thread t1 = new Thread(c1);
        Thread t2 = new Thread(c2);

        t1.start();
        t2.start();

        signaller.addListener(c1);
        signaller.addListener(c2);

        signaller.signal();

        callbacksCalled.await(250, TimeUnit.MILLISECONDS);

        t1.interrupt();
        t2.interrupt();
    }

    private static class CallbackTask implements Runnable, Signaller.SignalListener {
        private final CountDownLatch latch;

        public CallbackTask(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                Thread.yield();
            }
        }

        @Override
        public void onSignal() {
            latch.countDown();
        }
    }
}
