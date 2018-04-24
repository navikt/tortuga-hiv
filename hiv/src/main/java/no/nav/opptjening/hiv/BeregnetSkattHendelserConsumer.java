package no.nav.opptjening.hiv;

import io.prometheus.client.Counter;
import no.nav.opptjening.hiv.hendelser.CouldNotFindNextSekvensnummerRecord;
import no.nav.opptjening.hiv.hendelser.SekvensnummerReader;
import no.nav.opptjening.skatt.api.hendelser.HendelserClient;
import no.nav.opptjening.skatt.exceptions.ApiException;
import no.nav.opptjening.skatt.exceptions.EmptyResultException;
import no.nav.opptjening.skatt.schema.hendelsesliste.Hendelsesliste;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BeregnetSkattHendelserConsumer implements Runnable {

    private static final int ANTALL_HENDELSER_PER_REQUEST = 1000;

    private static final int POLL_TIMEOUT_MS = 5000;

    private static final long FIRST_VALID_SEKVENSNUMMER = 1;

    private static final Logger LOG = LoggerFactory.getLogger(BeregnetSkattHendelserConsumer.class);

    private static final Counter antallHendelserHentet = Counter.build()
            .name("hendelser_received")
            .help("Antall hendelser hentet.").register();

    private final HendelserClient beregnetskattHendelserClient;
    private final HendelseKafkaProducer hendelseProducer;
    private final SekvensnummerReader sekvensnummerReader;

    public BeregnetSkattHendelserConsumer(HendelserClient beregnetskattHendelserClient, HendelseKafkaProducer hendelseProducer,
                                          SekvensnummerReader sekvensnummerReader) {
        this.beregnetskattHendelserClient = beregnetskattHendelserClient;
        this.hendelseProducer = hendelseProducer;
        this.sekvensnummerReader = sekvensnummerReader;
    }

    public void run() {
        try {
            long nextSekvensnummer = sekvensnummerReader.readSekvensnummer();

            if (nextSekvensnummer == -1) {
                LOG.info("We did not find any nextSekvensnummer record, and assume that the log is empty." +
                        "Setting nextSekvensnummer={}", FIRST_VALID_SEKVENSNUMMER);
                nextSekvensnummer = FIRST_VALID_SEKVENSNUMMER;
            }

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Hendelsesliste hendelsesliste = beregnetskattHendelserClient.getHendelser(nextSekvensnummer, ANTALL_HENDELSER_PER_REQUEST);
                    antallHendelserHentet.inc(hendelsesliste.getHendelser().size());

                    LOG.info("Fetched {} hendelser", hendelsesliste.getHendelser().size());
                    long lastSentSekvensnummer = hendelseProducer.sendHendelser(hendelsesliste.getHendelser());

                    nextSekvensnummer = lastSentSekvensnummer + 1;
                } catch (EmptyResultException e) {
                    LOG.debug("Skatteetaten reported no new records, waiting a bit before trying again", e);
                    Thread.sleep(POLL_TIMEOUT_MS);
                } catch (ApiException e) {
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
        } catch (Exception e) {
            LOG.error("Unknown error", e);
        } finally {
            hendelseProducer.close();
        }

        LOG.info("Skatteetaten task stopped");
    }


}
