package no.nav.opptjening.hiv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ApplicationRunner {
    private static final int GRACEFUL_TERMINATION_TIMEOUT = 10000;

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationRunner.class);

    private final ExecutorService executorService;
    /**
     * a task that should run forever, or as long as the application should run
     */
    private final Runnable main;
    /**
     * other tasks that runs in the background. their exit status doesn't affect
     * the application
     */
    private final Runnable[] tasks;

    public ApplicationRunner(Runnable main, Runnable... tasks) {
        this.main = main;
        this.tasks = tasks;
        this.executorService = Executors.newFixedThreadPool(1 + tasks.length);

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdownHook));
    }

    private void shutdownHook() {
        LOG.debug("Received shutdown signal");
        shutdownAndAwaitTermination();
    }

    public void shutdownAndAwaitTermination() {
        executorService.shutdownNow();
        try {
            LOG.info("Waiting up to {} ms before shutting down", GRACEFUL_TERMINATION_TIMEOUT);
            if (executorService.awaitTermination(GRACEFUL_TERMINATION_TIMEOUT, TimeUnit.MILLISECONDS)) {
                LOG.info("Shutdown gracefully");
            } else {
                LOG.warn("Shutdown timeout");
            }
        } catch (InterruptedException e) {
            LOG.error("Got interrupted while awaiting termination", e);
        }
        LOG.info("Exiting app, goodbye");
    }

    public void run() {
        boolean shutdownOk = runTasksAndWait();

        if (!shutdownOk) {
            LOG.info("Stopping because main task shutdown");
        } else {
            LOG.info("Stopping because executor service shutdown");
        }

        LOG.info("Exiting main loop");
        System.exit(!shutdownOk ? 1 : 0);
    }

    private Future<?> scheduleTasks() {
        Future<?> future = executorService.submit(main);
        for (Runnable r : tasks) {
            executorService.submit(r);
        }
        return future;
    }

    private boolean runTasksAndWait() {
        Future<?> mainTask = scheduleTasks();

        while (!executorService.isShutdown() && !mainTask.isDone()) {
                /* do nothing while mainTask is running */
        }

        /* main task should never exit, only in case of failure */
        return !mainTask.isDone();
    }
}
