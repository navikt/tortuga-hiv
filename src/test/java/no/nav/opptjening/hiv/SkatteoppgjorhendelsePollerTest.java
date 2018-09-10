package no.nav.opptjening.hiv;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import no.nav.opptjening.hiv.sekvensnummer.SekvensnummerReader;
import no.nav.opptjening.skatt.client.Hendelsesliste;
import no.nav.opptjening.skatt.client.api.hendelseliste.HendelserClient;
import no.nav.opptjening.skatt.client.api.skatteoppgjoer.SkatteoppgjoerhendelserClient;
import no.nav.opptjening.skatt.client.schema.hendelsesliste.HendelseslisteDto;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SkatteoppgjorhendelsePollerTest {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule();

    private HendelserClient hendelserClient;

    @Before
    public void setUp() throws Exception {
        this.hendelserClient = new SkatteoppgjoerhendelserClient("http://localhost:" + wireMockRule.port() + "/", "apikey");
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

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(-1));
        Hendelsesliste hendelsesliste = skatteoppgjorhendelsePoller.poll();

        Assert.assertEquals(2, hendelsesliste.getHendelser().size());

        Assert.assertEquals(10, hendelsesliste.getHendelser().get(0).getSekvensnummer());
        Assert.assertEquals("12345", hendelsesliste.getHendelser().get(0).getIdentifikator());
        Assert.assertEquals("2016", hendelsesliste.getHendelser().get(0).getGjelderPeriode());

        Assert.assertEquals(11, hendelsesliste.getHendelser().get(1).getSekvensnummer());
        Assert.assertEquals("67891", hendelsesliste.getHendelser().get(1).getIdentifikator());
        Assert.assertEquals("2017", hendelsesliste.getHendelser().get(1).getGjelderPeriode());
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

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(10));
        Hendelsesliste hendelsesliste = skatteoppgjorhendelsePoller.poll();

        Assert.assertEquals(2, hendelsesliste.getHendelser().size());

        Assert.assertEquals(10, hendelsesliste.getHendelser().get(0).getSekvensnummer());
        Assert.assertEquals("12345", hendelsesliste.getHendelser().get(0).getIdentifikator());
        Assert.assertEquals("2016", hendelsesliste.getHendelser().get(0).getGjelderPeriode());

        Assert.assertEquals(11, hendelsesliste.getHendelser().get(1).getSekvensnummer());
        Assert.assertEquals("67891", hendelsesliste.getHendelser().get(1).getIdentifikator());
        Assert.assertEquals("2017", hendelsesliste.getHendelser().get(1).getGjelderPeriode());
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

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(1));

        Hendelsesliste hendelsesliste = skatteoppgjorhendelsePoller.poll();
        Assert.assertEquals(1, hendelsesliste.getHendelser().size());
        Assert.assertEquals(1, hendelsesliste.getHendelser().get(0).getSekvensnummer());
        Assert.assertEquals("12345", hendelsesliste.getHendelser().get(0).getIdentifikator());
        Assert.assertEquals("2016", hendelsesliste.getHendelser().get(0).getGjelderPeriode());


        hendelsesliste = skatteoppgjorhendelsePoller.poll();
        Assert.assertEquals(1, hendelsesliste.getHendelser().size());
        Assert.assertEquals(2, hendelsesliste.getHendelser().get(0).getSekvensnummer());
        Assert.assertEquals("67891", hendelsesliste.getHendelser().get(0).getIdentifikator());
        Assert.assertEquals("2017", hendelsesliste.getHendelser().get(0).getGjelderPeriode());
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

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(1));

        Hendelsesliste hendelsesliste = skatteoppgjorhendelsePoller.poll();
        Assert.assertEquals(2, hendelsesliste.getHendelser().size());
        Assert.assertEquals(1002, hendelsesliste.getHendelser().get(0).getSekvensnummer());
        Assert.assertEquals("12345", hendelsesliste.getHendelser().get(0).getIdentifikator());
        Assert.assertEquals("2016", hendelsesliste.getHendelser().get(0).getGjelderPeriode());

        Assert.assertEquals(1003, hendelsesliste.getHendelser().get(1).getSekvensnummer());
        Assert.assertEquals("67891", hendelsesliste.getHendelser().get(1).getIdentifikator());
        Assert.assertEquals("2017", hendelsesliste.getHendelser().get(1).getGjelderPeriode());
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

        SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller = new SkatteoppgjorhendelsePoller(hendelserClient, new ReturnSpecificSekvensnummer(1));

        try {
            Hendelsesliste hendelsesliste = skatteoppgjorhendelsePoller.poll();
            Assert.fail("Expected EmptyResultException to be thrown when nextSekvensnummer >= latestSekvensnummer");
        } catch (EmptyResultException e) {
            // ok
        }
    }

    private class ReturnSpecificSekvensnummer implements SekvensnummerReader {
        private final long sekvensnummer;

        public ReturnSpecificSekvensnummer(long sekvensnummer) {
            this.sekvensnummer = sekvensnummer;
        }

        @Override
        public long readSekvensnummer() {
            return sekvensnummer;
        }
    }
}
