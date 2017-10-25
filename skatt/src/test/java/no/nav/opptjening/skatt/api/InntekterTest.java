package no.nav.opptjening.skatt.api;

import no.nav.opptjening.skatt.dto.InntektDto;
import no.nav.opptjening.skatt.exceptions.MissingInntektException;
import no.nav.opptjening.skatt.exceptions.UnmappedException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import static org.junit.Assert.fail;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.*;

public class InntekterTest {
    private static final String TEST_API_URL = "http://testapi:8080/api/inntekter";

    private MockRestServiceServer mockServer;

    private Inntekter inntekter;


    @Before
    public void setUp() throws Exception {
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.setErrorHandler(new SkattErrorHandler());

        this.inntekter = new Inntekter(TEST_API_URL, restTemplate);
        this.mockServer = MockRestServiceServer.createServer(restTemplate);
    }

    @Test
    public void hentInntekt() throws Exception {
        mockServer.expect(requestTo(TEST_API_URL + "/2016/12345"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"personindentfikator\": \"12345\", \"inntektsaar\": \"2016\", \"pensjonsgivendeInntekt\": 150000}", MediaType.APPLICATION_JSON));

        InntektDto result = inntekter.hentInntekt("2016", "12345");

        mockServer.verify();

        Assert.assertEquals("12345", result.getPersonindentfikator());
        Assert.assertEquals("2016", result.getInntektsaar());
        Assert.assertEquals(150000, result.getPensjonsgivendeInntekt(), 0);
    }

    @Test
    public void hentInntektUnexpectedFormat() throws Exception {
        mockServer.expect(requestTo(TEST_API_URL + "/2016/12345"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"pensjonsgivendeInntekt\": 150000}", MediaType.APPLICATION_JSON));

        try {
            InntektDto result = inntekter.hentInntekt("2016", "12345");
            fail("Expected an HttpMessageNotReadableException to be thrown");
        } catch (HttpMessageNotReadableException e) {
            // ok
        }

        mockServer.verify();
    }

    @Test
    public void hentInntektThrowsApiException() throws Exception {
        mockServer.expect(requestTo(TEST_API_URL + "/2016/12345"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withBadRequest().body("{\"kode\": \"PIA-006\", \"melding\": \"Fant ikke PGI for gitt inntektsår og identifikator\"}").contentType(MediaType.APPLICATION_JSON));

        try {
            InntektDto result = inntekter.hentInntekt("2016", "12345");
            fail("Expected an MissingInntektException to be thrown");
        } catch (MissingInntektException e) {
            // ok
        }

        mockServer.verify();
    }

    @Test
    public void hentInntektThrowsUnmappable() throws Exception {
        mockServer.expect(requestTo(TEST_API_URL + "/2016/12345"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withBadRequest().body("{\"kode\": \"ZZ-001\", \"melding\": \"Denne feilkoden er ikke implementert\"}").contentType(MediaType.APPLICATION_JSON));

        try {
            InntektDto result = inntekter.hentInntekt("2016", "12345");
            fail("Expected an UnmappedException to be thrown");
        } catch (UnmappedException e) {
            // ok
        }

        mockServer.verify();
    }

    @Test
    public void hentInntektThrowsClientException() throws Exception {
        mockServer.expect(requestTo(TEST_API_URL + "/2016/12345"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withBadRequest().body("bad request").contentType(MediaType.TEXT_PLAIN));

        try {
            InntektDto result = inntekter.hentInntekt("2016", "12345");
            fail("Expected an HttpClientErrorException to be thrown");
        } catch (HttpClientErrorException e) {
            // ok
        }

        mockServer.verify();
    }

    @Test
    public void hentInntektThrowsServerException() throws Exception {
        mockServer.expect(requestTo(TEST_API_URL + "/2016/12345"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError().body("internal server error").contentType(MediaType.TEXT_PLAIN));

        try {
            InntektDto result = inntekter.hentInntekt("2016", "12345");
            fail("Expected an HttpServerErrorException to be thrown");
        } catch (HttpServerErrorException e) {
            // ok
        }

        mockServer.verify();
    }
}