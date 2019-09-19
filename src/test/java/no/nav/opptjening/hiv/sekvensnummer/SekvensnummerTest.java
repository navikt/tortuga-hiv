package no.nav.opptjening.hiv.sekvensnummer;

import com.github.tomakehurst.wiremock.WireMockServer;
import no.nav.opptjening.hiv.testsupport.SpecificSekvensnummerReader;
import no.nav.opptjening.skatt.client.api.hendelseliste.HendelserClient;
import no.nav.opptjening.skatt.client.api.skatteoppgjoer.SkatteoppgjoerhendelserClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static no.nav.opptjening.hiv.testsupport.SkeHendelseApiStubs.stubFirstSekvensnummerFromSkatteEtaten;
import static no.nav.opptjening.hiv.testsupport.SkeHendelseApiStubs.stubSekvensnummerLimit;
import static org.junit.jupiter.api.Assertions.*;

class SekvensnummerTest {

    private static final long SEKVENSNUMMER_FROM_TOPIC = 2L;
    private static final long SEKVENSNUMMER_FROM_SKATTEETATEN = 1L;
    private static final int LAST_PROCESSED_SEKVENSNUMMER = 1000;
    private static final long SEKVENSNUMMER_COUNT = 1L;
    private static final int FIRST_SEKVENSNUMMER_NOT_TO_PROCESS = 150;
    private static final long SPECIFIC_SEKVENSNUMER = 11745289L;
    private static HendelserClient hendelserClient;
    private static final WireMockServer wireMockServer = new WireMockServer(8080);
    private static final Supplier<LocalDate> SPECIFIC_DATE = () -> LocalDate.of(2019, 5, 6);

    @BeforeAll
    static void setUp() {
        wireMockServer.start();
        hendelserClient = new SkatteoppgjoerhendelserClient("http://localhost:" + wireMockServer.port() + "/", "apikey");
    }

    @AfterAll
    static void tearDown() {
        wireMockServer.stop();
    }

    @Test
    void next_returns_next_sekvensnummer_from_topic() {
        Sekvensnummer sekvensnummer = new Sekvensnummer(hendelserClient, new SpecificSekvensnummerReader(SEKVENSNUMMER_FROM_TOPIC), SPECIFIC_DATE);
        assertEquals(SEKVENSNUMMER_FROM_TOPIC, sekvensnummer.next());
    }

    @Test
    void next_returns_next_sekvensnummer_from_skatteetaten() {
        stubFirstSekvensnummerFromSkatteEtaten();
        Sekvensnummer sekvensnummer = new Sekvensnummer(hendelserClient, new SpecificSekvensnummerReader(null), SPECIFIC_DATE);
        assertEquals(SEKVENSNUMMER_FROM_SKATTEETATEN, sekvensnummer.next());
    }

    @Test
    void next_returns_next_sekvensnummer_when_incremented() {
        Sekvensnummer sekvensnummer = new Sekvensnummer(hendelserClient, new SpecificSekvensnummerReader(SEKVENSNUMMER_FROM_TOPIC), SPECIFIC_DATE);
        sekvensnummer.incrementSekvensnummer(SEKVENSNUMMER_COUNT);
        assertEquals(SEKVENSNUMMER_FROM_TOPIC + SEKVENSNUMMER_COUNT, sekvensnummer.next());
    }

    @Test
    void next_returns_next_sekvensnummer_when_updated() {
        Sekvensnummer sekvensnummer = new Sekvensnummer(hendelserClient, new SpecificSekvensnummerReader(SEKVENSNUMMER_FROM_TOPIC), SPECIFIC_DATE);
        sekvensnummer.updateProcessedSekvensnummer(LAST_PROCESSED_SEKVENSNUMMER);
        assertEquals(LAST_PROCESSED_SEKVENSNUMMER + 1, sekvensnummer.next());
    }

    @Test
    void sekvensnummer_limit() {
        stubSekvensnummerLimit(FIRST_SEKVENSNUMMER_NOT_TO_PROCESS, SPECIFIC_DATE.get());
        Sekvensnummer sekvensnummer = new Sekvensnummer(hendelserClient, new SpecificSekvensnummerReader(SEKVENSNUMMER_FROM_TOPIC), SPECIFIC_DATE);
        assertFalse(sekvensnummer.reachedSekvensnummerLimit());
        sekvensnummer.updateProcessedSekvensnummer(FIRST_SEKVENSNUMMER_NOT_TO_PROCESS - 1);
        assertTrue(sekvensnummer.reachedSekvensnummerLimit());
    }

    @Test
    void sekvensnummer_limit_cache_is_updated_when_day_has_passed() {
        stubSekvensnummerLimit(FIRST_SEKVENSNUMMER_NOT_TO_PROCESS, SPECIFIC_DATE.get());
        stubSekvensnummerLimit(FIRST_SEKVENSNUMMER_NOT_TO_PROCESS + 1, SPECIFIC_DATE.get().plusDays(1));
        AtomicReference<LocalDate> date = new AtomicReference<>(SPECIFIC_DATE.get());
        Sekvensnummer sekvensnummer = new Sekvensnummer(hendelserClient, new SpecificSekvensnummerReader(SEKVENSNUMMER_FROM_TOPIC), date::get);
        assertFalse(sekvensnummer.reachedSekvensnummerLimit());
        sekvensnummer.updateProcessedSekvensnummer(FIRST_SEKVENSNUMMER_NOT_TO_PROCESS - 1);
        assertTrue(sekvensnummer.reachedSekvensnummerLimit());
        date.updateAndGet(localdate -> localdate.plusDays(1));
        assertFalse(sekvensnummer.reachedSekvensnummerLimit());
    }
}
