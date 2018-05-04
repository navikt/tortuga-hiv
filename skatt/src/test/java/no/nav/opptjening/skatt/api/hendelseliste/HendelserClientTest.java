package no.nav.opptjening.skatt.api.hendelseliste;

import com.fasterxml.jackson.databind.ObjectMapper;
import no.nav.opptjening.skatt.api.beregnetskatt.BeregnetSkattHendelserClient;
import no.nav.opptjening.skatt.api.exceptions.ResponseUnmappableException;
import no.nav.opptjening.skatt.api.exceptions.UkjentFeilkodeException;
import no.nav.opptjening.skatt.api.hendelseliste.exceptions.MissingSekvensnummerException;
import no.nav.opptjening.skatt.exceptions.ClientException;
import no.nav.opptjening.skatt.exceptions.ServerException;
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
    public void when_NextSekvensnummerAfterDateResponseIsOk_Then_ValuesAreMappedCorrectly() throws Exception {
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
    public void when_NextSekvensnummerAfterDateResponseIsUnexpectedFormat_Then_ThrowResponseUnmappableException() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("{\"sekvens\": 10}")
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
        );

        try {
            LocalDate date = LocalDate.of(2017, 1, 1);
            hendelserClient.forsteSekvensEtter(date);
            fail("Expected an ResponseUnmappableException to be thrown");
        } catch (ResponseUnmappableException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/formueinntekt/beregnetskatt/hendelser/start?dato=2017-01-01", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test
    public void when_NextSekvensnummerAfterDateResponseIsGeneric4xxError_Then_ThrowClientException() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("this is not valid json")
                .setResponseCode(400)
                .addHeader("Content-Type", "application/json")
        );

        try {
            LocalDate date = LocalDate.of(2017, 1, 1);
            hendelserClient.forsteSekvensEtter(date);
            fail("Expected an ClientException to be thrown");
        } catch (ClientException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/formueinntekt/beregnetskatt/hendelser/start?dato=2017-01-01", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test
    public void when_HendelserResponseIsOk_Then_ValuesAreMappedCorrectly() throws Exception {
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
    public void when_HendelserResponseIsUnexpectedFormat_Then_ThrowResponseUnmappableException() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("{\"foo\": \"bar\"}")
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
        );

        try {
            hendelserClient.getHendelser(10, 1000);
            fail("Expected an ResponseUnmappableException to be thrown");
        } catch (ResponseUnmappableException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/formueinntekt/beregnetskatt/hendelser?fraSekvensnummer=10&antall=1000", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test
    public void when_fraSekvensnummerIsNotSet_Then_ThrowMissingSekvensnummerException() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("{\"kode\": \"FA-001\", \"melding\": \"fraSekvensnummer må være satt\"}")
                .setResponseCode(400)
                .addHeader("Content-Type", "application/json")
        );

        try {
            hendelserClient.getHendelser(10, 1000);
            fail("Expected an MissingSekvensnummerException to be thrown");
        } catch (MissingSekvensnummerException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/formueinntekt/beregnetskatt/hendelser?fraSekvensnummer=10&antall=1000", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test
    public void when_feilkodeIsUnknown_Then_ThrowUkjentFeilkodeException() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("{\"kode\": \"ZZ-001\", \"melding\": \"Denne feilkoden er ikke implementert\"}")
                .setResponseCode(400)
                .addHeader("Content-Type", "application/json")
        );

        try {
            hendelserClient.getHendelser(10, 1000);
            fail("Expected an UkjentFeilkodeException to be thrown");
        } catch (UkjentFeilkodeException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/formueinntekt/beregnetskatt/hendelser?fraSekvensnummer=10&antall=1000", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test
    public void when_HendelserResponseIsGeneric4xxError_Then_ThrowClientException() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("bad request")
                .setResponseCode(400)
                .addHeader("Content-Type", "text/plain")
        );

        try {
            hendelserClient.getHendelser(10, 1000);
            fail("Expected an ClientException to be thrown");
        } catch (ClientException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/formueinntekt/beregnetskatt/hendelser?fraSekvensnummer=10&antall=1000", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test
    public void when_HendelserResponseIsGeneric5xxError_Then_ThrowServerException() throws Exception {
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
