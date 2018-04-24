package no.nav.opptjening.skatt.api.beregnetskatt;

import com.fasterxml.jackson.databind.ObjectMapper;
import no.nav.opptjening.skatt.api.hendelser.HendelserClient;
import no.nav.opptjening.skatt.exceptions.*;
import no.nav.opptjening.skatt.schema.hendelsesliste.Hendelsesliste;
import no.nav.opptjening.skatt.schema.hendelsesliste.Sekvensnummer;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

public class HendelserClientTest {
    @Rule
    public MockWebServer server = new MockWebServer();

    private HendelserClient hendelserClient;

    @Before
    public void setUp() throws Exception {
        this.hendelserClient = new BeregnetSkattHendelserClient(server.url("/formueinntekt/beregnetskatt/").toString());
    }

    @Test
    public void forsteSekvensEtter() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("{\"sekvensnummer\": 10}")
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
        );

        LocalDate date = LocalDate.of(2017, 1, 1);
        Sekvensnummer result = hendelserClient.forsteSekvensEtter(date);

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/formueinntekt/beregnetskatt/hendelser/start?dato=2017-01-01", request.getPath());
        Assert.assertEquals("GET", request.getMethod());

        Assert.assertEquals(10, (long)result.getSekvensnummer());
    }

    @Test
    public void forsteSekvensEtterUnexpectedFormat() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("{\"sekvens\": 10}")
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
        );

        try {
            LocalDate date = LocalDate.of(2017, 1, 1);
            Sekvensnummer result = hendelserClient.forsteSekvensEtter(date);
            fail("Expected an ResponseMappingException to be thrown");
        } catch (ResponseMappingException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/formueinntekt/beregnetskatt/hendelser/start?dato=2017-01-01", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test
    public void forsteSekvensEtterInvalidJson() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("this is not valid json")
                .setResponseCode(400)
                .addHeader("Content-Type", "application/json")
        );

        try {
            LocalDate date = LocalDate.of(2017, 1, 1);
            Sekvensnummer result = hendelserClient.forsteSekvensEtter(date);
            fail("Expected an ClientException to be thrown");
        } catch (ClientException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/formueinntekt/beregnetskatt/hendelser/start?dato=2017-01-01", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test
    public void getHendelser() throws Exception {
        Map<String, List<HendelseDto>> response = new HashMap<>();

        List<HendelseDto> mockHendelser = new ArrayList<>();
        mockHendelser.add(new HendelseDto(10, "12345", "2016"));
        mockHendelser.add(new HendelseDto(11, "67891", "2017"));

        response.put("hendelser", mockHendelser);

        server.enqueue(new MockResponse()
                .setBody(new ObjectMapper().writeValueAsString(response))
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
        );

        Hendelsesliste result = hendelserClient.getHendelser(10, 1000);

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/formueinntekt/beregnetskatt/hendelser?fraSekvensnummer=10&antall=1000", request.getPath());
        Assert.assertEquals("GET", request.getMethod());

        Assert.assertEquals(2, result.getHendelser().size());

        Assert.assertEquals(10, (long)result.getHendelser().get(0).getSekvensnummer());
        Assert.assertEquals("12345", result.getHendelser().get(0).getIdentifikator());
        Assert.assertEquals("2016", result.getHendelser().get(0).getGjelderPeriode());

        Assert.assertEquals(11, (long)result.getHendelser().get(1).getSekvensnummer());
        Assert.assertEquals("67891", result.getHendelser().get(1).getIdentifikator());
        Assert.assertEquals("2017", result.getHendelser().get(1).getGjelderPeriode());
    }

    @Test
    public void getHendelserUnexpectedFormat() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("{\"foo\": \"bar\"}")
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
        );

        try {
            Hendelsesliste result = hendelserClient.getHendelser(10, 1000);
            fail("Expected an ResponseMappingException to be thrown");
        } catch (ResponseMappingException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/formueinntekt/beregnetskatt/hendelser?fraSekvensnummer=10&antall=1000", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test
    public void getHendelserThrowsApiException() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("{\"kode\": \"FA-001\", \"melding\": \"fraSekvensnummer må være satt\"}")
                .setResponseCode(400)
                .addHeader("Content-Type", "application/json")
        );

        try {
            Hendelsesliste result = hendelserClient.getHendelser(10, 1000);
            fail("Expected an MissingSekvensnummerException to be thrown");
        } catch (MissingSekvensnummerException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/formueinntekt/beregnetskatt/hendelser?fraSekvensnummer=10&antall=1000", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test
    public void getHendelserThrowsUnmappable() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("{\"kode\": \"ZZ-001\", \"melding\": \"Denne feilkoden er ikke implementert\"}")
                .setResponseCode(400)
                .addHeader("Content-Type", "application/json")
        );

        try {
            Hendelsesliste result = hendelserClient.getHendelser(10, 1000);
            fail("Expected an UnknownException to be thrown");
        } catch (UnknownException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/formueinntekt/beregnetskatt/hendelser?fraSekvensnummer=10&antall=1000", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test
    public void getHendelserThrowsClientException() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("bad request")
                .setResponseCode(400)
                .addHeader("Content-Type", "text/plain")
        );

        try {
            Hendelsesliste result = hendelserClient.getHendelser(10, 1000);
            fail("Expected an ClientException to be thrown");
        } catch (ClientException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/formueinntekt/beregnetskatt/hendelser?fraSekvensnummer=10&antall=1000", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test
    public void getHendelserThrowsServerException() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("internal server error")
                .setResponseCode(500)
                .addHeader("Content-Type", "text/plain")
        );

        try {
            Hendelsesliste result = hendelserClient.getHendelser(10, 1000);
            fail("Expected an ServerException to be thrown");
        } catch (ServerException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/formueinntekt/beregnetskatt/hendelser?fraSekvensnummer=10&antall=1000", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    private class HendelseDto {
        public final long sekvensnummer;
        public final String identifikator;
        public final String gjelderPeriode;

        public HendelseDto(long sekvensnummer, String identifikator, String gjelderPeriode) {
            this.sekvensnummer = sekvensnummer;
            this.identifikator = identifikator;
            this.gjelderPeriode = gjelderPeriode;
        }
    }
}
