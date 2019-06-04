package no.nav.opptjening.hiv;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import no.nav.opptjening.hiv.sekvensnummer.SekvensnummerReader;
import no.nav.opptjening.skatt.client.Hendelsesliste;
import no.nav.opptjening.skatt.client.Sekvensnummer;
import no.nav.opptjening.skatt.client.api.hendelseliste.HendelserClient;
import no.nav.opptjening.skatt.client.exceptions.HttpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

class SkatteoppgjorhendelsePoller {

    private static final Logger LOG = LoggerFactory.getLogger(SkatteoppgjorhendelsePoller.class);

    private final int antallHendelserPerRequest;
    private final HendelserClient beregnetskattHendelserClient;
    private final SekvensnummerReader sekvensnummerReader;
    private final Supplier<LocalDate> dateSupplier;
    private long nextSekvensnummer = -1;

    SkatteoppgjorhendelsePoller(HendelserClient beregnetskattHendelserClient, SekvensnummerReader sekvensnummerReader,
                                Supplier<LocalDate> dateSupplier, int antallHendelserPerRequest) {
        this.beregnetskattHendelserClient = beregnetskattHendelserClient;
        this.sekvensnummerReader = sekvensnummerReader;
        this.dateSupplier = dateSupplier;
        this.antallHendelserPerRequest = antallHendelserPerRequest;
        initNextSekvensnummer();
    }

    private void initNextSekvensnummer() {
        this.nextSekvensnummer = fetchSekvensnummerFromTopic().orElseGet(this::fetchSekvensnummerFromSkatteEtaten);
    }

    private Long fetchSekvensnummerFromSkatteEtaten() {
        try {
            long firstValidSekvensnummer = beregnetskattHendelserClient.forsteSekvensnummer().getSekvensnummer();
            LOG.info("We did not find any nextSekvensnummer record, and assume that the log is empty." +
                    "Setting nextSekvensnummer={}", firstValidSekvensnummer);
            return firstValidSekvensnummer;
        } catch (IOException e) { throw new RuntimeException(e); }
    }

    private Optional<Long> fetchSekvensnummerFromTopic() {
        var sekvensnummer = sekvensnummerReader.readSekvensnummer();
        sekvensnummer.ifPresent(it ->  LOG.info("Next sekvensnummer read from topic is: {}", it));
        return sekvensnummer;
    }


    List<Hendelsesliste.Hendelse> poll() throws IOException {

        pollCounter.inc();

        long sekvensnummerLimit = sekvensnummerLimit().getSekvensnummer();

        try {
            LOG.info("Latest sekvensnummer for date={} is {}.", dateSupplier.get(), sekvensnummerLimit);
            latestSekvensnummerGauge.set(sekvensnummerLimit);

            List<Hendelsesliste.Hendelse> hendelsesliste = fetchHendelserUntilLimit(sekvensnummerLimit);

            if (endOfHendelsesListe(sekvensnummerLimit, hendelsesliste)) {
                throw new EmptyResultException("We have reached the end of the hendelseliste");
            }

            antallHendelserHentetTotalt.inc(hendelsesliste.size());
            hendelsesliste.forEach(hendelse -> antallHendelserHentet.labels(hendelse.getGjelderPeriode()).inc());

            incrementNextSekvensnummer(hendelsesliste);

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

    private List<Hendelsesliste.Hendelse> fetchHendelserUntilLimit(long sekvensnummerLimit) throws IOException {
        while (!reachedSekvensnummerLimit(sekvensnummerLimit)) {
            var hendelsesliste = beregnetskattHendelserClient.getHendelser(nextSekvensnummer, antallHendelserPerRequest).getHendelser();
            LOG.info("Fetched {} hendelser", hendelsesliste.size());
            if (hendelsesliste.isEmpty()) {
                nextSekvensnummer = nextSekvensnummer + antallHendelserPerRequest;
            } else{
                return hendelsesliste;
            }
        }
        return Collections.emptyList();
    }

    private void incrementNextSekvensnummer(List<Hendelsesliste.Hendelse> hendelsesliste) {
        long lastReceivedSekvensnummer = hendelsesliste.get(hendelsesliste.size() - 1).getSekvensnummer();
        nextSekvensnummer = lastReceivedSekvensnummer + 1;
    }

    private boolean endOfHendelsesListe(long sekvensnummerLimit, List<Hendelsesliste.Hendelse> hendelsesliste) {
        return hendelsesliste.isEmpty() || reachedSekvensnummerLimit(sekvensnummerLimit);
    }

    private boolean reachedSekvensnummerLimit(long sekvensnummerLimit) {
        return nextSekvensnummer >= sekvensnummerLimit;
    }

    private Sekvensnummer sekvensnummerLimit() throws IOException {
        return beregnetskattHendelserClient.forsteSekvensnummerEtter(dateSupplier.get());
    }

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
}
