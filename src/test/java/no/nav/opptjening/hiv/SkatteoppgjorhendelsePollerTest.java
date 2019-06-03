package no.nav.opptjening.hiv;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import no.nav.opptjening.hiv.sekvensnummer.SekvensnummerReader;
import no.nav.opptjening.skatt.client.api.hendelseliste.HendelserClient;
import no.nav.opptjening.skatt.client.api.skatteoppgjoer.SkatteoppgjoerhendelserClient;
import no.nav.opptjening.skatt.client.schema.hendelsesliste.HendelseslisteDto;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.*;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class SkatteoppgjorhendelsePollerTest {

    private static final String ANTALL_HENDELSER_PER_REQUEST = "1000";
    private static final Supplier<LocalDate> SPECIFIC_DATE = () -> LocalDate.of(2019, 5, 6);
    private static final String HENDELSER_PATH = "/hendelser/";
    private static final String API_KEY = "apikey";
    private static final String FIRST_SEKVENSNUMMER = "10";
    public static final int LAST_LEGAL_SEKVENSNUMMER = 11;
    private static HendelserClient hendelserClient;
    private static WireMockServer wireMockServer = new WireMockServer(8080);

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
    void that_ReadingStartsAtFirstValidSekvensnummer_When_NoSekvensnummerAreStored() throws Exception {
        String validSekvensnummer = "10";
        stubFirstSekvensnummer();
        stubLastLegalSekvensnummer(LAST_LEGAL_SEKVENSNUMMER);

        var hendelseMockList = List.of(
                new HendelseslisteDto.HendelseDto(10, "12345", "2016"),
                new HendelseslisteDto.HendelseDto(11, "67891", "2017")
        );


        stubHendelser(validSekvensnummer, getJsonMockHendelser(hendelseMockList));

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(null), SPECIFIC_DATE);
        var hendelsesliste = skatteoppgjorhendelsePoller.poll();

        assertEquals(2, hendelsesliste.size());

        assertEquals(10, hendelsesliste.get(0).getSekvensnummer());
        assertEquals("12345", hendelsesliste.get(0).getIdentifikator());
        assertEquals("2016", hendelsesliste.get(0).getGjelderPeriode());

        assertEquals(11, hendelsesliste.get(1).getSekvensnummer());
        assertEquals("67891", hendelsesliste.get(1).getIdentifikator());
        assertEquals("2017", hendelsesliste.get(1).getGjelderPeriode());
    }

    @Test
    void that_ReadingStartsAtStoredSekvensnummer() throws Exception {
        String validSekvensnummer = "10";
        stubLastLegalSekvensnummer(LAST_LEGAL_SEKVENSNUMMER);

        var hendelseMockList = List.of(
                new HendelseslisteDto.HendelseDto(10, "12345", "2016"),
                new HendelseslisteDto.HendelseDto(11, "67891", "2017"));

        stubHendelser(validSekvensnummer, getJsonMockHendelser(hendelseMockList));

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(10L), SPECIFIC_DATE);
        var hendelsesliste = skatteoppgjorhendelsePoller.poll();

        assertEquals(2, hendelsesliste.size());

        assertEquals(10, hendelsesliste.get(0).getSekvensnummer());
        assertEquals("12345", hendelsesliste.get(0).getIdentifikator());
        assertEquals("2016", hendelsesliste.get(0).getGjelderPeriode());

        assertEquals(11, hendelsesliste.get(1).getSekvensnummer());
        assertEquals("67891", hendelsesliste.get(1).getIdentifikator());
        assertEquals("2017", hendelsesliste.get(1).getGjelderPeriode());
    }


    @Test
    public void that_ReadingContinuesWithTheLastSekvensnummerPlusOne() throws Exception {

        stubLastLegalSekvensnummer(LAST_LEGAL_SEKVENSNUMMER);
        stubHendelser("1", getJsonMockHendelser(List.of(new HendelseslisteDto.HendelseDto(1, "12345", "2016"))));
        stubHendelser("2", getJsonMockHendelser(List.of(new HendelseslisteDto.HendelseDto(2, "67891", "2017"))));

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(1L), SPECIFIC_DATE);

        var hendelsesliste = skatteoppgjorhendelsePoller.poll();
        assertEquals(1, hendelsesliste.size());
        assertEquals(1, hendelsesliste.get(0).getSekvensnummer());
        assertEquals("12345", hendelsesliste.get(0).getIdentifikator());
        assertEquals("2016", hendelsesliste.get(0).getGjelderPeriode());


        hendelsesliste = skatteoppgjorhendelsePoller.poll();
        assertEquals(1, hendelsesliste.size());
        assertEquals(2, hendelsesliste.get(0).getSekvensnummer());
        assertEquals("67891", hendelsesliste.get(0).getIdentifikator());
        assertEquals("2017", hendelsesliste.get(0).getGjelderPeriode());
    }

    @Test
    public void that_ReadingContinuesWithTheLastSekvensnummerPlusAntallIfNoRecordsAreReturned() throws Exception {
        stubLastLegalSekvensnummer(1003);
        stubHendelser("1", getJsonMockHendelser(Collections.emptyList()));
        stubHendelser("1002", getJsonMockHendelser(List.of(
                new HendelseslisteDto.HendelseDto(1002, "12345", "2016"),
                new HendelseslisteDto.HendelseDto(1003, "67891", "2017"))));



        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(1L), SPECIFIC_DATE);

        var hendelsesliste = skatteoppgjorhendelsePoller.poll();
        assertEquals(2, hendelsesliste.size());
        assertEquals(1002, hendelsesliste.get(0).getSekvensnummer());
        assertEquals("12345", hendelsesliste.get(0).getIdentifikator());
        assertEquals("2016", hendelsesliste.get(0).getGjelderPeriode());

        assertEquals(1003, hendelsesliste.get(1).getSekvensnummer());
        assertEquals("67891", hendelsesliste.get(1).getIdentifikator());
        assertEquals("2017", hendelsesliste.get(1).getGjelderPeriode());
    }

    @Test
    public void that_ReadingStopsWhenLatestSekvensnummerIsReached() throws Exception {
        stubLastLegalSekvensnummer(1);
        stubHendelser("1", getJsonMockHendelser(Collections.emptyList()));

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(1L), SPECIFIC_DATE);

        assertThrows(EmptyResultException.class, skatteoppgjorhendelsePoller::poll,"Expected EmptyResultException to be thrown when nextSekvensnummer >= sekvensnummerLimit");
    }

    private void stubFirstSekvensnummer() {
        WireMock.stubFor(WireMock.get(WireMock.urlEqualTo("/hendelser/start"))
                .withHeader("X-Nav-Apikey", WireMock.equalTo("apikey"))
                .willReturn(WireMock.okJson("{\"sekvensnummer\":" + FIRST_SEKVENSNUMMER + "}")));
    }

    private void stubLastLegalSekvensnummer(int sekvensnummerLimit) {
        WireMock.stubFor(WireMock.get(WireMock.urlPathEqualTo("/hendelser/start"))
                .withQueryParam("dato", WireMock.equalTo(SPECIFIC_DATE.get().toString()))
                .withHeader("X-Nav-Apikey", WireMock.equalTo("apikey"))
                .willReturn(WireMock.okJson("{\"sekvensnummer\": " + sekvensnummerLimit + "}")));
    }

    private void stubHendelser(String fraSekvensnummer, String jsonHendelse) {
        WireMock.stubFor(WireMock.get(WireMock.urlPathEqualTo(HENDELSER_PATH))
                .withQueryParam("fraSekvensnummer", WireMock.equalTo(fraSekvensnummer))
                .withQueryParam("antall", WireMock.equalTo(ANTALL_HENDELSER_PER_REQUEST))
                .withHeader("X-Nav-Apikey", WireMock.equalTo(API_KEY))
                .willReturn(WireMock.okJson(jsonHendelse)));
    }

    private String getJsonMockHendelser(List<HendelseslisteDto.HendelseDto> mockHendelser) throws JsonProcessingException {
        Map<String, List<HendelseslisteDto.HendelseDto>> response = new HashMap<>();
        response.put("hendelser", mockHendelser);

        return new ObjectMapper().writeValueAsString(response);
    }

    private class ReturnSpecificSekvensnummer implements SekvensnummerReader {
        private final Long sekvensnummer;

        public ReturnSpecificSekvensnummer(Long sekvensnummer) {
            this.sekvensnummer = sekvensnummer;
        }

        @Override
        public Optional<Long> readSekvensnummer() {
            return Optional.ofNullable(sekvensnummer);
        }
    }
}
