package no.nav.opptjening.hiv;

import com.github.tomakehurst.wiremock.WireMockServer;
import no.nav.opptjening.hiv.sekvensnummer.Sekvensnummer;
import no.nav.opptjening.hiv.sekvensnummer.SekvensnummerReader;
import no.nav.opptjening.skatt.client.api.hendelseliste.HendelserClient;
import no.nav.opptjening.skatt.client.api.skatteoppgjoer.SkatteoppgjoerhendelserClient;

import no.nav.opptjening.skatt.client.exceptions.HttpException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.*;
import java.util.function.Supplier;

import static no.nav.opptjening.hiv.SkeHendelseApiStubs.*;
import static no.nav.opptjening.skatt.client.schema.hendelsesliste.HendelseslisteDto.*;
import static org.junit.jupiter.api.Assertions.*;

class SkatteoppgjorhendelsePollerTest {

    private static final int ANTALL_HENDELSER_PER_REQUEST = 1000;
    private static final Supplier<LocalDate> SPECIFIC_DATE = () -> LocalDate.of(2019, 5, 6);
    private static HendelserClient hendelserClient;
    private static final WireMockServer wireMockServer = new WireMockServer(8080);

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
    void reading_starts_at_stored_sekvensnummer() throws Exception {
        SekvensnummerReader storedSekvensnummer = new SpecificSekvensnummer(10L);
        String validSekvensnummer = "10";
        stubSekvensnummerLimit(11, SPECIFIC_DATE.get());

        var expectedHendelser = List.of(
                new HendelseDto(10, "12345", "2016"),
                new HendelseDto(11, "67891", "2017")
        );

        stubHendelser(validSekvensnummer, ANTALL_HENDELSER_PER_REQUEST, getJsonMockHendelser(expectedHendelser));

        Sekvensnummer sekvensnummer = new Sekvensnummer(hendelserClient, storedSekvensnummer, SPECIFIC_DATE);
        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, sekvensnummer, ANTALL_HENDELSER_PER_REQUEST);
        var hendelsesliste = skatteoppgjorhendelsePoller.poll();

        assertHendelser(expectedHendelser, hendelsesliste);
    }

    @Test
    void reading_continues_with_the_last_sekvensnummer_plus_one() throws Exception {
        Sekvensnummer sekvensnummer = new Sekvensnummer(hendelserClient, new SpecificSekvensnummer(1L), SPECIFIC_DATE);
        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, sekvensnummer, ANTALL_HENDELSER_PER_REQUEST);

        stubSekvensnummerLimit(3, SPECIFIC_DATE.get());

        var expectedHendelse = List.of(new HendelseDto(1, "12345", "2016"));
        stubHendelser("1", ANTALL_HENDELSER_PER_REQUEST,  getJsonMockHendelser(expectedHendelse));

        var hendelser = skatteoppgjorhendelsePoller.poll();
        assertHendelser(expectedHendelse, hendelser);

        expectedHendelse = List.of(new HendelseDto(2, "67891", "2017"));
        stubHendelser("2", ANTALL_HENDELSER_PER_REQUEST, getJsonMockHendelser(expectedHendelse));

        hendelser = skatteoppgjorhendelsePoller.poll();
        assertHendelser(expectedHendelse, hendelser);
    }

    @Test
    void reading_handles_holes_in_the_hendelselist() throws Exception {

        stubSekvensnummerLimit(1003, SPECIFIC_DATE.get());

        stubHendelser("1", ANTALL_HENDELSER_PER_REQUEST, getJsonMockHendelser(Collections.emptyList()));
        var expectedHendelser = List.of(
                new HendelseDto(1001, "67891", "2015"),
                new HendelseDto(1002, "12345", "2016")
        );
        stubHendelser("1001", ANTALL_HENDELSER_PER_REQUEST, getJsonMockHendelser(expectedHendelser));
        stubHendelser("1002", ANTALL_HENDELSER_PER_REQUEST, getJsonMockHendelser(List.of(
                new HendelseDto(1002, "12345", "2016")
        )));

        Sekvensnummer sekvensnummer = new Sekvensnummer(hendelserClient, new SpecificSekvensnummer(1L), SPECIFIC_DATE);
        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, sekvensnummer, ANTALL_HENDELSER_PER_REQUEST);

        var hendelser = skatteoppgjorhendelsePoller.poll();
        assertHendelser(expectedHendelser, hendelser);
    }

    @Test
    void reads_hendelser_until_sekvensnummerLimit_is_reached() throws Exception {
        var expectedHendelser = List.of(
                new HendelseDto(1, "67891", "2015"),
                new HendelseDto(999, "67891", "2015"),
                new HendelseDto(1000, "12345", "2016"),
                new HendelseDto(1001, "12345", "2016"),
                new HendelseDto(2060, "12345", "2016"),
                new HendelseDto(3000, "12345", "2016"),
                new HendelseDto(3003, "12345", "2016")
        );

        stubFirstSekvensnummerFromSkatteEtaten();
        stubSekvensnummerLimit(3003, SPECIFIC_DATE.get());
        stubHendelser("1", ANTALL_HENDELSER_PER_REQUEST, getJsonMockHendelser(expectedHendelser));

        Sekvensnummer sekvensnummer = new Sekvensnummer(hendelserClient, new SpecificSekvensnummer(1L), SPECIFIC_DATE);
        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, sekvensnummer, ANTALL_HENDELSER_PER_REQUEST);        var actualHendelser = skatteoppgjorhendelsePoller.poll();
        assertHendelser(expectedHendelser, actualHendelser);
    }

    @Test
    void reading_starts_at_first_sekvensnummer_when_fetchSekvensnummerFromTopic_returns_null() throws Exception {
        var expectedHendelser = List.of(
                new HendelseDto(1, "12345", "2016"),
                new HendelseDto(5, "89012", "2018"),
                new HendelseDto(11, "67891", "2017")
        );

        String fromSekvensnummer = "1";
        stubFirstSekvensnummerFromSkatteEtaten();
        stubSekvensnummerLimit(11, SPECIFIC_DATE.get());

        stubHendelser(fromSekvensnummer, ANTALL_HENDELSER_PER_REQUEST, getJsonMockHendelser(expectedHendelser));

        Sekvensnummer sekvensnummer = new Sekvensnummer(hendelserClient, new SpecificSekvensnummer(null), SPECIFIC_DATE);
        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, sekvensnummer, ANTALL_HENDELSER_PER_REQUEST);        var actualHendelser = skatteoppgjorhendelsePoller.poll();

        assertHendelser(expectedHendelser, actualHendelser);
    }

    @Test
    void reads_hendelser_until_sekvensnummerLimit_is_reached_with_pagination_and_1_hendelse_per_request() throws Exception {
        var expectedHendelser = List.of(
                new HendelseDto(1, "89012", "2017"),
                new HendelseDto(2, "67891", "2015"),
                new HendelseDto(3, "12345", "2016")
        );

        stubFirstSekvensnummerFromSkatteEtaten();

        int sekvensnummerLimit = 4;
        stubSekvensnummerLimit(sekvensnummerLimit, SPECIFIC_DATE.get());

        int fromSekvensnummer = 1;
        int amountHendelserPerRequest = 1;
        stubHendelseFeed(fromSekvensnummer, amountHendelserPerRequest, expectedHendelser);

        Sekvensnummer sekvensnummer = new Sekvensnummer(hendelserClient, new SpecificSekvensnummer(1L), SPECIFIC_DATE);
        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, sekvensnummer, 1);
        assertHendelse(expectedHendelser.get(0), skatteoppgjorhendelsePoller.poll().get(0));
        assertHendelse(expectedHendelser.get(1), skatteoppgjorhendelsePoller.poll().get(0));
        assertHendelse(expectedHendelser.get(2), skatteoppgjorhendelsePoller.poll().get(0));
        assertThrows(EmptyResultException.class, skatteoppgjorhendelsePoller::poll);
    }

    @Test
    void poll_throws_httpException_when_status_code_4xx_is_returned() {
        Sekvensnummer sekvensnummer = new Sekvensnummer(hendelserClient, new SpecificSekvensnummer(1L), SPECIFIC_DATE);
        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, sekvensnummer, ANTALL_HENDELSER_PER_REQUEST);
        stub400statusCodesFromSkatteEtaten(SPECIFIC_DATE.get());

        assertThrows(HttpException.class, skatteoppgjorhendelsePoller::poll);
    }

    @Test
    void poll_throws_httpException_when_status_code_5xx_is_returned() {
        Sekvensnummer sekvensnummer = new Sekvensnummer(hendelserClient, new SpecificSekvensnummer(1L), SPECIFIC_DATE);
        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, sekvensnummer, ANTALL_HENDELSER_PER_REQUEST);        stub500statusCodesFromSkatteEtaten();
        assertThrows(HttpException.class, skatteoppgjorhendelsePoller::poll);
    }

    @Test
    void reading_stops_when_the_sekvensnummerLimit_is_reached() throws Exception {
        stubSekvensnummerLimit(1, SPECIFIC_DATE.get());
        stubHendelser("1", ANTALL_HENDELSER_PER_REQUEST, getJsonMockHendelser(Collections.emptyList()));

        Sekvensnummer sekvensnummer = new Sekvensnummer(hendelserClient, new SpecificSekvensnummer(1L), SPECIFIC_DATE);
        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, sekvensnummer, ANTALL_HENDELSER_PER_REQUEST);
        assertThrows(EmptyResultException.class, skatteoppgjorhendelsePoller::poll);
    }

    @Test
    void reading_stops_when_the_end_of_hendelsesliste_is_reached() throws Exception {
        stubSekvensnummerLimit(5, SPECIFIC_DATE.get());
        stubHendelser("1", ANTALL_HENDELSER_PER_REQUEST, getJsonMockHendelser(Collections.emptyList()));

        Sekvensnummer sekvensnummer = new Sekvensnummer(hendelserClient, new SpecificSekvensnummer(1L), SPECIFIC_DATE);
        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, sekvensnummer, ANTALL_HENDELSER_PER_REQUEST);
        assertThrows(EmptyResultException.class, skatteoppgjorhendelsePoller::poll);
    }

    private class SpecificSekvensnummer implements SekvensnummerReader {
        private final Long sekvensnummer;

        private SpecificSekvensnummer(Long sekvensnummer) {
            this.sekvensnummer = sekvensnummer;
        }

        @Override
        public Optional<Long> readSekvensnummer() {
            return Optional.ofNullable(sekvensnummer);
        }
    }
}
