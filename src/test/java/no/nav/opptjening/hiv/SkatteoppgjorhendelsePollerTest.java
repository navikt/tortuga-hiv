package no.nav.opptjening.hiv;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import no.nav.opptjening.hiv.sekvensnummer.SekvensnummerReader;
import no.nav.opptjening.skatt.client.Hendelsesliste;
import no.nav.opptjening.skatt.client.api.hendelseliste.HendelserClient;
import no.nav.opptjening.skatt.client.api.skatteoppgjoer.SkatteoppgjoerhendelserClient;
import no.nav.opptjening.skatt.client.schema.hendelsesliste.HendelseslisteDto;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.LocalDate;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class SkatteoppgjorhendelsePollerTest {

    private static HendelserClient hendelserClient;
    private static WireMockServer wireMockServer = new WireMockServer(8080);

    @BeforeAll
    public static void setUp() {
        wireMockServer.start();
        hendelserClient = new SkatteoppgjoerhendelserClient("http://localhost:" + wireMockServer.port() + "/", "apikey");
    }

    @AfterAll
    public static void tearDown() {
        wireMockServer.stop();
    }

    @Test
    public void that_ReadingStartsAtFirstValidSekvensnummer_When_NoSekvensnummerAreStored() throws Exception {
        WireMock.stubFor(WireMock.get(WireMock.urlEqualTo("/hendelser/start"))
                .withHeader("X-Nav-Apikey", WireMock.equalTo("apikey"))
                .willReturn(WireMock.okJson("{\"sekvensnummer\": 10}")));
        WireMock.stubFor(WireMock.get(WireMock.urlPathEqualTo("/hendelser/start"))
                .withQueryParam("dato", WireMock.equalTo(LocalDate.now().toString()))
                .withHeader("X-Nav-Apikey", WireMock.equalTo("apikey"))
                .willReturn(WireMock.okJson("{\"sekvensnummer\": 11}")));

        Map<String, List<HendelseslisteDto.HendelseDto>> response = new HashMap<>();

        List<HendelseslisteDto.HendelseDto> mockHendelser = new ArrayList<>();
        mockHendelser.add(new HendelseslisteDto.HendelseDto(10, "12345", "2016"));
        mockHendelser.add(new HendelseslisteDto.HendelseDto(11, "67891", "2017"));

        response.put("hendelser", mockHendelser);

        String jsonBody = new ObjectMapper().writeValueAsString(response);
        WireMock.stubFor(WireMock.get(WireMock.urlPathEqualTo("/hendelser/"))
                .withQueryParam("fraSekvensnummer", WireMock.equalTo("10"))
                .withQueryParam("antall", WireMock.equalTo("1000"))
                .withHeader("X-Nav-Apikey", WireMock.equalTo("apikey"))
                .willReturn(WireMock.okJson(jsonBody)));

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(null));
        Hendelsesliste hendelsesliste = skatteoppgjorhendelsePoller.poll();

        assertEquals(2, hendelsesliste.getHendelser().size());

        assertEquals(10, hendelsesliste.getHendelser().get(0).getSekvensnummer());
        assertEquals("12345", hendelsesliste.getHendelser().get(0).getIdentifikator());
        assertEquals("2016", hendelsesliste.getHendelser().get(0).getGjelderPeriode());

        assertEquals(11, hendelsesliste.getHendelser().get(1).getSekvensnummer());
        assertEquals("67891", hendelsesliste.getHendelser().get(1).getIdentifikator());
        assertEquals("2017", hendelsesliste.getHendelser().get(1).getGjelderPeriode());
    }

    @Test
    public void that_ReadingStartsAtStoredSekvensnummer() throws Exception {
        Map<String, List<HendelseslisteDto.HendelseDto>> response = new HashMap<>();

        List<HendelseslisteDto.HendelseDto> mockHendelser = new ArrayList<>();
        mockHendelser.add(new HendelseslisteDto.HendelseDto(10, "12345", "2016"));
        mockHendelser.add(new HendelseslisteDto.HendelseDto(11, "67891", "2017"));

        response.put("hendelser", mockHendelser);

        String jsonBody = new ObjectMapper().writeValueAsString(response);
        WireMock.stubFor(WireMock.get(WireMock.urlPathEqualTo("/hendelser/"))
                .withQueryParam("fraSekvensnummer", WireMock.equalTo("10"))
                .withQueryParam("antall", WireMock.equalTo("1000"))
                .withHeader("X-Nav-Apikey", WireMock.equalTo("apikey"))
                .willReturn(WireMock.okJson(jsonBody)));
        WireMock.stubFor(WireMock.get(WireMock.urlPathEqualTo("/hendelser/start"))
                .withQueryParam("dato", WireMock.equalTo(LocalDate.now().toString()))
                .withHeader("X-Nav-Apikey", WireMock.equalTo("apikey"))
                .willReturn(WireMock.okJson("{\"sekvensnummer\": 12}")));

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(10L));
        Hendelsesliste hendelsesliste = skatteoppgjorhendelsePoller.poll();

        assertEquals(2, hendelsesliste.getHendelser().size());

        assertEquals(10, hendelsesliste.getHendelser().get(0).getSekvensnummer());
        assertEquals("12345", hendelsesliste.getHendelser().get(0).getIdentifikator());
        assertEquals("2016", hendelsesliste.getHendelser().get(0).getGjelderPeriode());

        assertEquals(11, hendelsesliste.getHendelser().get(1).getSekvensnummer());
        assertEquals("67891", hendelsesliste.getHendelser().get(1).getIdentifikator());
        assertEquals("2017", hendelsesliste.getHendelser().get(1).getGjelderPeriode());
    }

    @Test
    public void that_ReadingContinuesWithTheLastSekvensnummerPlusOne() throws Exception {
        Map<String, List<HendelseslisteDto.HendelseDto>> response = new HashMap<>();

        List<HendelseslisteDto.HendelseDto> mockHendelser = new ArrayList<>();
        mockHendelser.add(new HendelseslisteDto.HendelseDto(1, "12345", "2016"));

        response.put("hendelser", mockHendelser);

        String jsonBody = new ObjectMapper().writeValueAsString(response);
        WireMock.stubFor(WireMock.get(WireMock.urlPathEqualTo("/hendelser/"))
                .withQueryParam("fraSekvensnummer", WireMock.equalTo("1"))
                .withQueryParam("antall", WireMock.equalTo("1000"))
                .withHeader("X-Nav-Apikey", WireMock.equalTo("apikey"))
                .willReturn(WireMock.okJson(jsonBody)));
        WireMock.stubFor(WireMock.get(WireMock.urlPathEqualTo("/hendelser/start"))
                .withQueryParam("dato", WireMock.equalTo(LocalDate.now().toString()))
                .withHeader("X-Nav-Apikey", WireMock.equalTo("apikey"))
                .willReturn(WireMock.okJson("{\"sekvensnummer\": 3}")));

        mockHendelser = new ArrayList<>();
        mockHendelser.add(new HendelseslisteDto.HendelseDto(2, "67891", "2017"));

        response.put("hendelser", mockHendelser);

        jsonBody = new ObjectMapper().writeValueAsString(response);
        WireMock.stubFor(WireMock.get(WireMock.urlPathEqualTo("/hendelser/"))
                .withQueryParam("fraSekvensnummer", WireMock.equalTo("2"))
                .withQueryParam("antall", WireMock.equalTo("1000"))
                .withHeader("X-Nav-Apikey", WireMock.equalTo("apikey"))
                .willReturn(WireMock.okJson(jsonBody)));

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(1L));

        Hendelsesliste hendelsesliste = skatteoppgjorhendelsePoller.poll();
        assertEquals(1, hendelsesliste.getHendelser().size());
        assertEquals(1, hendelsesliste.getHendelser().get(0).getSekvensnummer());
        assertEquals("12345", hendelsesliste.getHendelser().get(0).getIdentifikator());
        assertEquals("2016", hendelsesliste.getHendelser().get(0).getGjelderPeriode());


        hendelsesliste = skatteoppgjorhendelsePoller.poll();
        assertEquals(1, hendelsesliste.getHendelser().size());
        assertEquals(2, hendelsesliste.getHendelser().get(0).getSekvensnummer());
        assertEquals("67891", hendelsesliste.getHendelser().get(0).getIdentifikator());
        assertEquals("2017", hendelsesliste.getHendelser().get(0).getGjelderPeriode());
    }

    @Test
    public void that_ReadingContinuesWithTheLastSekvensnummerPlusAntallIfNoRecordsAreReturned() throws Exception {
        Map<String, List<HendelseslisteDto.HendelseDto>> response = new HashMap<>();

        List<HendelseslisteDto.HendelseDto> mockHendelser = new ArrayList<>();
        response.put("hendelser", mockHendelser);

        String jsonBody = new ObjectMapper().writeValueAsString(response);
        WireMock.stubFor(WireMock.get(WireMock.urlPathEqualTo("/hendelser/"))
                .withQueryParam("fraSekvensnummer", WireMock.equalTo("1"))
                .withQueryParam("antall", WireMock.equalTo("1000"))
                .withHeader("X-Nav-Apikey", WireMock.equalTo("apikey"))
                .willReturn(WireMock.okJson(jsonBody)));
        WireMock.stubFor(WireMock.get(WireMock.urlPathEqualTo("/hendelser/start"))
                .withQueryParam("dato", WireMock.equalTo(LocalDate.now().toString()))
                .withHeader("X-Nav-Apikey", WireMock.equalTo("apikey"))
                .willReturn(WireMock.okJson("{\"sekvensnummer\": 1003}")));

        mockHendelser.add(new HendelseslisteDto.HendelseDto(1002, "12345", "2016"));
        mockHendelser.add(new HendelseslisteDto.HendelseDto(1003, "67891", "2017"));
        response.put("hendelser", mockHendelser);

        jsonBody = new ObjectMapper().writeValueAsString(response);
        WireMock.stubFor(WireMock.get(WireMock.urlPathEqualTo("/hendelser/"))
                .withQueryParam("fraSekvensnummer", WireMock.equalTo("1002"))
                .withQueryParam("antall", WireMock.equalTo("1000"))
                .withHeader("X-Nav-Apikey", WireMock.equalTo("apikey"))
                .willReturn(WireMock.okJson(jsonBody)));

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(1L));

        Hendelsesliste hendelsesliste = skatteoppgjorhendelsePoller.poll();
        assertEquals(2, hendelsesliste.getHendelser().size());
        assertEquals(1002, hendelsesliste.getHendelser().get(0).getSekvensnummer());
        assertEquals("12345", hendelsesliste.getHendelser().get(0).getIdentifikator());
        assertEquals("2016", hendelsesliste.getHendelser().get(0).getGjelderPeriode());

        assertEquals(1003, hendelsesliste.getHendelser().get(1).getSekvensnummer());
        assertEquals("67891", hendelsesliste.getHendelser().get(1).getIdentifikator());
        assertEquals("2017", hendelsesliste.getHendelser().get(1).getGjelderPeriode());
    }

    @Test
    public void that_ReadingStopsWhenLatestSekvensnummerIsReached() throws Exception {
        Map<String, List<HendelseslisteDto.HendelseDto>> response = new HashMap<>();

        List<HendelseslisteDto.HendelseDto> mockHendelser = new ArrayList<>();
        response.put("hendelser", mockHendelser);

        String jsonBody = new ObjectMapper().writeValueAsString(response);
        WireMock.stubFor(WireMock.get(WireMock.urlPathEqualTo("/hendelser/"))
                .withQueryParam("fraSekvensnummer", WireMock.equalTo("1"))
                .withQueryParam("antall", WireMock.equalTo("1000"))
                .withHeader("X-Nav-Apikey", WireMock.equalTo("apikey"))
                .willReturn(WireMock.okJson(jsonBody)));
        WireMock.stubFor(WireMock.get(WireMock.urlPathEqualTo("/hendelser/start"))
                .withQueryParam("dato", WireMock.equalTo(LocalDate.now().toString()))
                .withHeader("X-Nav-Apikey", WireMock.equalTo("apikey"))
                .willReturn(WireMock.okJson("{\"sekvensnummer\": 1}")));

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(1L));

        try {
            Hendelsesliste hendelsesliste = skatteoppgjorhendelsePoller.poll();
            fail("Expected EmptyResultException to be thrown when nextSekvensnummer >= latestSekvensnummer");
        } catch (EmptyResultException e) {
            // ok
        }
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
