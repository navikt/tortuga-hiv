package no.nav.opptjening.hiv;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import no.nav.opptjening.hiv.sekvensnummer.SekvensnummerReader;
import no.nav.opptjening.skatt.client.Hendelsesliste;
import no.nav.opptjening.skatt.client.Sekvensnummer;
import no.nav.opptjening.skatt.client.api.hendelseliste.HendelserClient;
import no.nav.opptjening.skatt.client.exceptions.HttpException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;

public class SkatteoppgjorhendelsePoller {

    private static final Logger LOG = LoggerFactory.getLogger(SkatteoppgjorhendelsePoller.class);

    private static final int ANTALL_HENDELSER_PER_REQUEST = 1000;

    private static final Counter pollCounter = Counter.build()
            .name("hendelser_poll_count")
            .help("Hvor mange ganger vi pollet etter hendelser").register();

    private static final Gauge nextSekvensnummerGauge = Gauge.build()
            .name("next_sekvensnummer")
            .help("Neste sekvensnummer vi forventer å få hendelser på").register();

    private static final Gauge latestSekvensnummerGauge = Gauge.build()
            .name("latest_sekvensnummer")
            .help("Siste sekvensnummer for dagens dato").register();

    private static final Counter antallHendelserHentet = Counter.build()
            .name("hendelser_received")
            .labelNames("year")
            .help("Antall hendelser hentet.").register();

    private static final Counter antallHendelserHentetTotalt = Counter.build()
            .name("hendelser_received_total")
            .help("Antall hendelser hentet totalt.").register();

    private static final Gauge skatteetatenEmptyResultCounter = Gauge.build()
            .name("skatteetaten_empty_result_count")
            .help("Hvor mange ganger vi fikk et tomt resultat tilbake fra Skatteetaten").register();

    private static final Counter skatteetatenErrorCounter = Counter.build()
            .name("skatteetaten_error_count")
            .help("Antall feil vi har fått fra Skatteetaten sitt API").register();

    private final HendelserClient beregnetskattHendelserClient;
    private final SekvensnummerReader sekvensnummerReader;
    private long nextSekvensnummer = -1;

    public SkatteoppgjorhendelsePoller(@NotNull HendelserClient beregnetskattHendelserClient, @NotNull SekvensnummerReader sekvensnummerReader) {
        this.beregnetskattHendelserClient = beregnetskattHendelserClient;
        this.sekvensnummerReader = sekvensnummerReader;
    }

    @NotNull
    public Hendelsesliste poll() throws IOException {
        if (nextSekvensnummer == -1) {
            this.nextSekvensnummer = sekvensnummerReader.readSekvensnummer();
            LOG.info("Next sekvensnummer read from topic is: {}", nextSekvensnummer);

            if (nextSekvensnummer == -1) {
                Sekvensnummer firstValidSekvensnummer = beregnetskattHendelserClient.forsteSekvensnummer();
                LOG.info("We did not find any nextSekvensnummer record, and assume that the log is empty." +
                        "Setting nextSekvensnummer={}", firstValidSekvensnummer.getSekvensnummer());
                nextSekvensnummer = firstValidSekvensnummer.getSekvensnummer();
            }
        }

        pollCounter.inc();

        try {
            Sekvensnummer latestSekvensnummer = beregnetskattHendelserClient.forsteSekvensnummerEtter(LocalDate.now());
            LOG.info("Latest sekvensnummer for date={} is {}.", LocalDate.now(), latestSekvensnummer);
            latestSekvensnummerGauge.set(latestSekvensnummer.getSekvensnummer());

            Hendelsesliste hendelsesliste = null;

            while (nextSekvensnummer <= latestSekvensnummer.getSekvensnummer()) {
                hendelsesliste = beregnetskattHendelserClient.getHendelser(nextSekvensnummer, ANTALL_HENDELSER_PER_REQUEST);

                LOG.info("Fetched {} hendelser", hendelsesliste.getHendelser().size());

                if (hendelsesliste.getHendelser().size() > 0) {
                    break;
                }

                nextSekvensnummer = nextSekvensnummer + ANTALL_HENDELSER_PER_REQUEST + 1;
            }

            if (hendelsesliste == null || nextSekvensnummer > latestSekvensnummer.getSekvensnummer()) {
                throw new EmptyResultException("We have reached the end of the hendelseliste");
            }


            antallHendelserHentetTotalt.inc(hendelsesliste.getHendelser().size());
            hendelsesliste.getHendelser()
                    .forEach(hendelse -> antallHendelserHentet.labels(hendelse.getGjelderPeriode()).inc());

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
