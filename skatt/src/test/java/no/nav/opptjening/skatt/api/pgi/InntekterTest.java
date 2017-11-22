package no.nav.opptjening.skatt.api.pgi;

import no.nav.opptjening.skatt.exceptions.*;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.fail;

public class InntekterTest {
    @Rule
    public MockWebServer server = new MockWebServer();

    private Inntekter inntekter;

    @Before
    public void setUp() throws Exception {
        this.inntekter = new Inntekter(server.url("/").toString());
    }

    @Test
    public void hentInntekt() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("{\"personindentifikator\": \"12345\", \"inntektsaar\": \"2016\", \"pensjonsgivendeInntekt\": 150000}")
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
        );

        InntektDto result = inntekter.hentInntekt("2016", "12345");

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/2016/12345", request.getPath());
        Assert.assertEquals("GET", request.getMethod());

        Assert.assertEquals("12345", result.getPersonindentfikator());
        Assert.assertEquals("2016", result.getInntektsaar());
        Assert.assertEquals(150000, result.getPensjonsgivendeInntekt(), 0);
    }

    @Test
    public void hentInntektUnexpectedFormat() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("{\"pensjonsgivendeInntekt\": 150000}")
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
        );

        try {
            InntektDto result = inntekter.hentInntekt("2016", "12345");
            fail("Expected an ResponseMappingException to be thrown");
        } catch (ResponseMappingException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/2016/12345", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test
    public void hentInntektThrowsApiException() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("{\"kode\": \"PIA-006\", \"melding\": \"Fant ikke PGI for gitt inntektsår og identifikator\"}")
                .setResponseCode(400)
                .addHeader("Content-Type", "application/json")
        );

        try {
            InntektDto result = inntekter.hentInntekt("2016", "12345");
            fail("Expected an MissingInntektException to be thrown");
        } catch (MissingInntektException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/2016/12345", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test
    public void hentInntektThrowsUnmappable() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("{\"kode\": \"ZZ-001\", \"melding\": \"Denne feilkoden er ikke implementert\"}")
                .setResponseCode(400)
                .addHeader("Content-Type", "application/json")
        );

        try {
            InntektDto result = inntekter.hentInntekt("2016", "12345");
            fail("Expected an UnknownException to be thrown");
        } catch (UnknownException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/2016/12345", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test
    public void hentInntektThrowsClientException() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("bad request")
                .setResponseCode(400)
                .addHeader("Content-Type", "text/plan")
        );

        try {
            InntektDto result = inntekter.hentInntekt("2016", "12345");
            fail("Expected an ClientException to be thrown");
        } catch (ClientException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/2016/12345", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test
    public void hentInntektThrowsServerException() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("internal server error")
                .setResponseCode(500)
                .addHeader("Content-Type", "text/plan")
        );

        try {
            InntektDto result = inntekter.hentInntekt("2016", "12345");
            fail("Expected an ServerException to be thrown");
        } catch (ServerException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/2016/12345", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }
}