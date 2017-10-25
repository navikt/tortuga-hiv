package no.nav.opptjening.skatt.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import no.nav.opptjening.skatt.dto.HendelseDto;
import no.nav.opptjening.skatt.dto.SekvensDto;
import no.nav.opptjening.skatt.exceptions.MissingSekvensnummerException;
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

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withBadRequest;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

public class HendelserTest {
    private static final String TEST_API_URL = "http://testapi:8080/api/hendelser";

    private MockRestServiceServer mockServer;

    private InntektHendelser hendelser;

    @Before
    public void setUp() throws Exception {
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.setErrorHandler(new SkattErrorHandler());

        this.hendelser = new InntektHendelser(TEST_API_URL, restTemplate);
        this.mockServer = MockRestServiceServer.createServer(restTemplate);
    }

    @Test
    public void forsteSekvensEtter() throws Exception {
        mockServer.expect(requestTo(TEST_API_URL + "/start?dato=2017-01-01"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"sekvensnummer\": 10}", MediaType.APPLICATION_JSON));

        LocalDate date = LocalDate.of(2017, 1, 1);
        SekvensDto result = hendelser.forsteSekvensEtter(date);

        mockServer.verify();

        Assert.assertEquals(10, result.getSekvensnummer());
    }

    @Test
    public void forsteSekvensEtterUnexpectedFormat() throws Exception {
        mockServer.expect(requestTo(TEST_API_URL + "/start?dato=2017-01-01"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"sekvens\": 10}", MediaType.APPLICATION_JSON));

        try {
            LocalDate date = LocalDate.of(2017, 1, 1);
            SekvensDto result = hendelser.forsteSekvensEtter(date);
            fail("Expected an HttpMessageNotReadableException to be thrown");
        } catch (HttpMessageNotReadableException e) {
            // ok
        }

        mockServer.verify();
    }

    @Test
    public void getHendelser() throws Exception {
        Map<String, List<HendelseDto>> response = new HashMap<>();

        List<HendelseDto> mockHendelser = new ArrayList<>();
        mockHendelser.add(new HendelseDto(10, "12345", "2016"));
        mockHendelser.add(new HendelseDto(11, "67891", "2017"));

        response.put("hendelser", mockHendelser);

        mockServer.expect(requestTo(TEST_API_URL + "?fraSekvensnummer=10&antall=1000"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(new ObjectMapper().writeValueAsString(response), MediaType.APPLICATION_JSON));

        List<HendelseDto> result = hendelser.getHendelser(10, 1000);

        mockServer.verify();

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
        mockServer.expect(requestTo(TEST_API_URL + "?fraSekvensnummer=10&antall=1000"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"foo\": \"bar\"}", MediaType.APPLICATION_JSON));

        try {
            List<HendelseDto> result = hendelser.getHendelser(10, 1000);
            fail("Expected an HttpMessageNotReadableException to be thrown");
        } catch (HttpMessageNotReadableException e) {
            // ok
        }

        mockServer.verify();
    }

    @Test
    public void getHendelserThrowsApiException() throws Exception {
        mockServer.expect(requestTo(TEST_API_URL + "?fraSekvensnummer=10&antall=1000"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withBadRequest().body("{\"kode\": \"FA-001\", \"melding\": \"fraSekvensnummer må være satt\"}").contentType(MediaType.APPLICATION_JSON));

        try {
            List<HendelseDto> result = hendelser.getHendelser(10, 1000);
            fail("Expected an MissingSekvensnummerException to be thrown");
        } catch (MissingSekvensnummerException e) {
            // ok
        }

        mockServer.verify();
    }

    @Test
    public void getHendelserThrowsUnmappable() throws Exception {
        mockServer.expect(requestTo(TEST_API_URL + "?fraSekvensnummer=10&antall=1000"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withBadRequest().body("{\"kode\": \"ZZ-001\", \"melding\": \"Denne feilkoden er ikke implementert\"}").contentType(MediaType.APPLICATION_JSON));

        try {
            List<HendelseDto> result = hendelser.getHendelser(10, 1000);
            fail("Expected an UnmappedException to be thrown");
        } catch (UnmappedException e) {
            // ok
        }

        mockServer.verify();
    }

    @Test
    public void getHendelserThrowsClientException() throws Exception {
        mockServer.expect(requestTo(TEST_API_URL + "?fraSekvensnummer=10&antall=1000"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withBadRequest().body("bad request").contentType(MediaType.TEXT_PLAIN));

        try {
            List<HendelseDto> result = hendelser.getHendelser(10, 1000);
            fail("Expected an HttpClientErrorException to be thrown");
        } catch (HttpClientErrorException e) {
            // ok
        }

        mockServer.verify();
    }

    @Test
    public void getHendelserThrowsServerException() throws Exception {
        mockServer.expect(requestTo(TEST_API_URL + "?fraSekvensnummer=10&antall=1000"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError().body("internal server error").contentType(MediaType.TEXT_PLAIN));

        try {
            List<HendelseDto> result = hendelser.getHendelser(10, 1000);
            fail("Expected an HttpServerErrorException to be thrown");
        } catch (HttpServerErrorException e) {
            // ok
        }

        mockServer.verify();
    }
}