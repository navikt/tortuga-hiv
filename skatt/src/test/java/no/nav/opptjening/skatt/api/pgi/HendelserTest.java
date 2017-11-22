package no.nav.opptjening.skatt.api.pgi;

import com.fasterxml.jackson.databind.ObjectMapper;
import no.nav.opptjening.skatt.api.hendelser.Hendelser;
import no.nav.opptjening.skatt.api.hendelser.HendelseDto;
import no.nav.opptjening.skatt.api.hendelser.SekvensDto;
import no.nav.opptjening.skatt.exceptions.*;
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

public class HendelserTest {
    @Rule
    public MockWebServer server = new MockWebServer();

    private Hendelser hendelser;

    @Before
    public void setUp() throws Exception {
        this.hendelser = new InntektHendelser(server.url("/").toString());
    }

    @Test
    public void forsteSekvensEtter() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("{\"sekvensnummer\": 10}")
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
        );

        LocalDate date = LocalDate.of(2017, 1, 1);
        SekvensDto result = hendelser.forsteSekvensEtter(date);

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/formueinntekt/beregnetskatt/hendelser/start?dato=2017-01-01", request.getPath());
        Assert.assertEquals("GET", request.getMethod());

        Assert.assertEquals(10, result.getSekvensnummer());
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
            SekvensDto result = hendelser.forsteSekvensEtter(date);
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
            SekvensDto result = hendelser.forsteSekvensEtter(date);
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

        List<HendelseDto> result = hendelser.getHendelser(10, 1000);

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/formueinntekt/beregnetskatt/hendelser?fraSekvensnummer=10&antall=1000", request.getPath());
        Assert.assertEquals("GET", request.getMethod());

        Assert.assertEquals(2, result.size());

        Assert.assertEquals(10, result.get(0).getSekvensnummer());
        Assert.assertEquals("12345", result.get(0).getIdentifikator());
        Assert.assertEquals("2016", result.get(0).getGjelderPeriode());

        Assert.assertEquals(11, result.get(1).getSekvensnummer());
        Assert.assertEquals("67891", result.get(1).getIdentifikator());
        Assert.assertEquals("2017", result.get(1).getGjelderPeriode());
    }

    @Test
    public void getHendelserUnexpectedFormat() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("{\"foo\": \"bar\"}")
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
        );

        try {
            List<HendelseDto> result = hendelser.getHendelser(10, 1000);
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
            List<HendelseDto> result = hendelser.getHendelser(10, 1000);
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
            List<HendelseDto> result = hendelser.getHendelser(10, 1000);
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
            List<HendelseDto> result = hendelser.getHendelser(10, 1000);
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
            List<HendelseDto> result = hendelser.getHendelser(10, 1000);
            fail("Expected an ServerException to be thrown");
        } catch (ServerException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/formueinntekt/beregnetskatt/hendelser?fraSekvensnummer=10&antall=1000", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }
}