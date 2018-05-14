package no.nav.opptjening.hiv.signals;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class BlockingSignallerTest {
    @Test
    public void signal() throws InterruptedException {
        Signaller.BlockingSignaller signaller = new Signaller.BlockingSignaller();
        final CountDownLatch threadStarted = new CountDownLatch(1);
        final CountDownLatch threadStopped = new CountDownLatch(1);
        new Thread(() -> {
            try {
                threadStarted.countDown();
                signaller.waitForSignal();
                threadStopped.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        threadStarted.await(500, TimeUnit.MILLISECONDS);

        signaller.signal();

        threadStopped.await(500, TimeUnit.MILLISECONDS);
    }
}
