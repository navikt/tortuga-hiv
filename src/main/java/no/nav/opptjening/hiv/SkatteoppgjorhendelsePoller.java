package no.nav.opptjening.hiv;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import no.nav.opptjening.hiv.sekvensnummer.SekvensnummerReader;
import no.nav.opptjening.skatt.api.hendelseliste.HendelserClient;
import no.nav.opptjening.skatt.api.hendelseliste.exceptions.EmptyResultException;
import no.nav.opptjening.skatt.exceptions.HttpException;
import no.nav.opptjening.skatt.schema.hendelsesliste.Hendelsesliste;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SkatteoppgjorhendelsePoller {

    private static final Logger LOG = LoggerFactory.getLogger(SkatteoppgjorhendelsePoller.class);

    private static final int ANTALL_HENDELSER_PER_REQUEST = 1000;
    private static final long FIRST_VALID_SEKVENSNUMMER = 1;

    private static final Counter pollCounter = Counter.build()
            .name("hendelser_poll_count")
            .help("Hvor mange ganger vi pollet etter hendelser").register();

    private static final Gauge nextSekvensnummerGauge = Gauge.build()
            .name("next_sekvensnummer")
            .help("Neste sekvensnummer vi forventer å få hendelser på").register();

    private static final Counter antallHendelserHentet = Counter.build()
            .name("hendelser_received")
            .help("Antall hendelser hentet.").register();

    private static final Gauge skatteetatenEmptyResultCounter = Gauge.build()
            .name("skatteetaten_empty_result_count")
            .help("Hvor mange ganger vi fikk et tomt resultat tilbake fra Skatteetaten").register();

    private static final Counter skatteetatenErrorCounter = Counter.build()
            .name("skatteetaten_error_count")
            .help("Antall feil vi har fått fra Skatteetaten sitt API").register();

    private final HendelserClient beregnetskattHendelserClient;
    private final SekvensnummerReader sekvensnummerReader;
    private long nextSekvensnummer = -1;

    public SkatteoppgjorhendelsePoller(HendelserClient beregnetskattHendelserClient, SekvensnummerReader sekvensnummerReader) {
        this.beregnetskattHendelserClient = beregnetskattHendelserClient;
        this.sekvensnummerReader = sekvensnummerReader;
    }

    public Hendelsesliste poll() throws IOException {
        if (nextSekvensnummer == -1) {
            this.nextSekvensnummer = sekvensnummerReader.readSekvensnummer();

            if (nextSekvensnummer == -1) {
                LOG.info("We did not find any nextSekvensnummer record, and assume that the log is empty." +
                        "Setting nextSekvensnummer={}", FIRST_VALID_SEKVENSNUMMER);
                nextSekvensnummer = FIRST_VALID_SEKVENSNUMMER;
            }
        }

        pollCounter.inc();

        try {
            Hendelsesliste hendelsesliste = beregnetskattHendelserClient.getHendelser(nextSekvensnummer, ANTALL_HENDELSER_PER_REQUEST);
            LOG.info("Fetched {} hendelser", hendelsesliste.getHendelser().size());
            antallHendelserHentet.inc(hendelsesliste.getHendelser().size());

            long lastReceivedSekvensnummer = hendelsesliste.getHendelser()
                    .get(hendelsesliste.getHendelser().size() - 1)
                    .getSekvensnummer();
            nextSekvensnummer = lastReceivedSekvensnummer + 1;

            return hendelsesliste;
        } catch (EmptyResultException e) {
            skatteetatenEmptyResultCounter.inc();
            throw e;
        } catch (HttpException e) {
            skatteetatenErrorCounter.inc();
            throw e;
        } finally {
            nextSekvensnummerGauge.set(nextSekvensnummer);
        }
    }
}