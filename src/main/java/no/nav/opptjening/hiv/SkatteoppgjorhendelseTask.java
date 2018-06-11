package no.nav.opptjening.hiv;

import no.nav.opptjening.hiv.sekvensnummer.CouldNotFindNextSekvensnummerRecord;
import no.nav.opptjening.skatt.client.Hendelsesliste;
import no.nav.opptjening.skatt.client.exceptions.HttpException;
import org.apache.kafka.common.KafkaException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SkatteoppgjorhendelseTask {

    private static final int POLL_TIMEOUT_MS = 5000;

    private static final Logger LOG = LoggerFactory.getLogger(SkatteoppgjorhendelseTask.class);

    private final SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller;
    private final SkatteoppgjorhendelseProducer hendelseProducer;

    public SkatteoppgjorhendelseTask(@NotNull SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller, @NotNull SkatteoppgjorhendelseProducer hendelseProducer) {
        this.skatteoppgjorhendelsePoller = skatteoppgjorhendelsePoller;
        this.hendelseProducer = hendelseProducer;
    }

    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Hendelsesliste hendelsesliste = skatteoppgjorhendelsePoller.poll();

                    hendelseProducer.sendHendelser(hendelsesliste.getHendelser());
                } catch (EmptyResultException e) {
                    LOG.debug("Skatteetaten reported no new records, waiting a bit before trying again", e);
                    Thread.sleep(POLL_TIMEOUT_MS);
                } catch (HttpException e) {
                    LOG.error("Error while contacting Skatteetaten", e);
                    Thread.sleep(POLL_TIMEOUT_MS);
                }
            }

            if (Thread.currentThread().isInterrupted()) {
                LOG.warn("Thread got interrupted, exiting");
            }
        } catch (IOException e) {
            LOG.warn("IO exception, exiting", e);
        } catch (InterruptedException e) {
            LOG.warn("Thread got interrupted during sleep, exiting", e);
        } catch (CouldNotFindNextSekvensnummerRecord e) {
            LOG.error(e.getMessage(), e);
        } catch (KafkaException e) {
            LOG.error("Error while consuming or producing data on Kafka", e);
        } catch (Exception e) {
            LOG.error("Unknown error", e);
        }

        LOG.info("Skatteetaten task stopped");
    }
}
