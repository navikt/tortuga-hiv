package no.nav.opptjening.hiv;

import com.github.tomakehurst.wiremock.WireMockServer;
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
    void that_ReadingStartsAtStoredSekvensnummer() throws Exception {
        String validSekvensnummer = "10";
        stubSekvensnummerLimit(11, SPECIFIC_DATE.get());

        var hendelseMockList = List.of(
                new HendelseDto(10, "12345", "2016"),
                new HendelseDto(11, "67891", "2017")
        );

        stubHendelser(validSekvensnummer, ANTALL_HENDELSER_PER_REQUEST, getJsonMockHendelser(hendelseMockList));

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(10L), SPECIFIC_DATE, ANTALL_HENDELSER_PER_REQUEST);
        var hendelsesliste = skatteoppgjorhendelsePoller.poll();

        assertHendelser(hendelseMockList, hendelsesliste);
    }


    @Test
    void that_ReadingContinuesWithTheLastSekvensnummerPlusOne() throws Exception {
        stubSekvensnummerLimit(11, SPECIFIC_DATE.get());
        List<HendelseDto> hendelser1 = List.of(new HendelseDto(1, "12345", "2016"));
        stubHendelser("1", ANTALL_HENDELSER_PER_REQUEST,  getJsonMockHendelser(hendelser1));
        List<HendelseDto> hendelser2 = List.of(new HendelseDto(2, "67891", "2017"));
        stubHendelser("2", ANTALL_HENDELSER_PER_REQUEST, getJsonMockHendelser(hendelser2));

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(1L), SPECIFIC_DATE, ANTALL_HENDELSER_PER_REQUEST);

        var hendelsesliste = skatteoppgjorhendelsePoller.poll();
        assertHendelser(hendelser1, hendelsesliste);

        hendelsesliste = skatteoppgjorhendelsePoller.poll();
        assertHendelser(hendelser2, hendelsesliste);
    }

    @Test
    void lol() throws Exception {
        stubSekvensnummerLimit(1003, SPECIFIC_DATE.get());
        stubHendelser("1", ANTALL_HENDELSER_PER_REQUEST, getJsonMockHendelser(Collections.emptyList()));
        stubHendelser("1001", ANTALL_HENDELSER_PER_REQUEST, getJsonMockHendelser(List.of(
                new HendelseDto(1001, "67891", "2015"),
                new HendelseDto(1002, "12345", "2016")
        )));
        stubHendelser("1002", ANTALL_HENDELSER_PER_REQUEST, getJsonMockHendelser(List.of(
                new HendelseDto(1002, "12345", "2016")
        )));

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(1L), SPECIFIC_DATE, ANTALL_HENDELSER_PER_REQUEST);

        var hendelsesliste = skatteoppgjorhendelsePoller.poll();
        assertEquals(2, hendelsesliste.size());
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

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(1L), SPECIFIC_DATE, ANTALL_HENDELSER_PER_REQUEST);
        var actualHendelser = skatteoppgjorhendelsePoller.poll();
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

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(null), SPECIFIC_DATE, ANTALL_HENDELSER_PER_REQUEST);
        var actualHendelser = skatteoppgjorhendelsePoller.poll();

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

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(1L), SPECIFIC_DATE, 1);

        assertHendelse(expectedHendelser.get(0), skatteoppgjorhendelsePoller.poll().get(0));
        assertHendelse(expectedHendelser.get(1), skatteoppgjorhendelsePoller.poll().get(0));
        assertHendelse(expectedHendelser.get(2), skatteoppgjorhendelsePoller.poll().get(0));
        assertThrows(EmptyResultException.class, skatteoppgjorhendelsePoller::poll);
    }

    @Test
    void poll_throws_httpException_when_status_code_4xx_is_returned() {
        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(1L), SPECIFIC_DATE, ANTALL_HENDELSER_PER_REQUEST);

        stub400statusCodesFromSkatteEtaten(SPECIFIC_DATE.get());

        assertThrows(HttpException.class, skatteoppgjorhendelsePoller::poll);
    }

    @Test
    void poll_throws_httpException_when_status_code_5xx_is_returned() {
        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(1L), SPECIFIC_DATE, ANTALL_HENDELSER_PER_REQUEST);

        stub500statusCodesFromSkatteEtaten(SPECIFIC_DATE.get());

        assertThrows(HttpException.class, skatteoppgjorhendelsePoller::poll);
    }

    @Test
    void reading_stops_when_the_sekvensnummerLimit_is_reached() throws Exception {
        stubSekvensnummerLimit(1, SPECIFIC_DATE.get());
        stubHendelser("1", ANTALL_HENDELSER_PER_REQUEST, getJsonMockHendelser(Collections.emptyList()));

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(1L), SPECIFIC_DATE, ANTALL_HENDELSER_PER_REQUEST);

        assertThrows(EmptyResultException.class, skatteoppgjorhendelsePoller::poll);
    }

    @Test
    void reading_stops_when_the_end_of_hendelsesliste_is_reached() throws Exception {
        stubSekvensnummerLimit(5, SPECIFIC_DATE.get());
        stubHendelser("1", ANTALL_HENDELSER_PER_REQUEST, getJsonMockHendelser(Collections.emptyList()));

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(1L), SPECIFIC_DATE, ANTALL_HENDELSER_PER_REQUEST);

        assertThrows(EmptyResultException.class, skatteoppgjorhendelsePoller::poll);
    }

    private class ReturnSpecificSekvensnummer implements SekvensnummerReader {
        private final Long sekvensnummer;

        private ReturnSpecificSekvensnummer(Long sekvensnummer) {
            this.sekvensnummer = sekvensnummer;
        }

        @Override
        public Optional<Long> readSekvensnummer() {
            return Optional.ofNullable(sekvensnummer);
        }
    }
}
