package no.nav.opptjening.hiv.sekvensnummer;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Optional;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.client.Gauge;

import no.nav.opptjening.skatt.client.api.hendelseliste.HendelserClient;

public class Sekvensnummer {

    private static final Logger LOG = LoggerFactory.getLogger(Sekvensnummer.class);

    private final HendelserClient beregnetskattHendelserClient;
    private final SekvensnummerReader sekvensnummerReader;
    private long nextSekvensnummer;
    private Cache<LocalDate, Long> cachedSekvensnummerLimit;
    private Supplier<LocalDate> dateSupplier;


    public Sekvensnummer(HendelserClient beregnetskattHendelserClient, SekvensnummerReader sekvensnummerReader, Supplier<LocalDate> dateSupplier) {
        this.beregnetskattHendelserClient = beregnetskattHendelserClient;
        this.sekvensnummerReader = sekvensnummerReader;
        this.nextSekvensnummer = initNextSekvensnummer();
        this.dateSupplier = dateSupplier;
        this.cachedSekvensnummerLimit = new Cache<>(this::fetchSekvensnummerLimit);
    }

    private long fetchSekvensnummerLimit(LocalDate date) {
        try {
            long sekvensnummerLimit =  beregnetskattHendelserClient.forsteSekvensnummerEtter(date).getSekvensnummer();
            latestSekvensnummerGauge.set(sekvensnummerLimit);
            return sekvensnummerLimit;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected Long initNextSekvensnummer() {
        Long nextSekvensnummer = fetchSekvensnummerFromTopic().orElseGet(this::fetchFirstSekvensnummerFromSkatteEtaten);
        nextSekvensnummerGauge.set(nextSekvensnummer);
        return nextSekvensnummer;
    }

    private long fetchFirstSekvensnummerFromSkatteEtaten() {
        try {
            long firstValidSekvensnummer = beregnetskattHendelserClient.forsteSekvensnummer().getSekvensnummer();
            LOG.info("We did not find any nextSekvensnummer record, and assume that the log is empty." +
                    "Setting nextSekvensnummer={}", firstValidSekvensnummer);
            return firstValidSekvensnummer;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Optional<Long> fetchSekvensnummerFromTopic() {
        var sekvensnummer = sekvensnummerReader.readSekvensnummer();
        sekvensnummer.ifPresent(it -> LOG.info("Next sekvensnummer read from topic is: {}", it));
        return sekvensnummer;
    }

    public void updateProcessedSekvensnummer(long lastProcessedSekvensnummer) {
        nextSekvensnummer = lastProcessedSekvensnummer + 1;
        nextSekvensnummerGauge.set(nextSekvensnummer);
    }

    public void incrementSekvensnummer(long sekvensnummerCount) {
        nextSekvensnummer += sekvensnummerCount;
        nextSekvensnummerGauge.set(nextSekvensnummer);
    }

    public boolean reachedSekvensnummerLimit() {
        return nextSekvensnummer >= cachedSekvensnummerLimit.apply(dateSupplier.get());
    }

    public long next() {
        return nextSekvensnummer;
    }

    protected static final Gauge nextSekvensnummerGauge = Gauge.build()
            .name("next_sekvensnummer")
            .help("Neste sekvensnummer vi forventer å få hendelser på").register();

    private static final Gauge latestSekvensnummerGauge = Gauge.build()
            .name("latest_sekvensnummer")
            .help("Siste sekvensnummer for dagens dato").register();

}
