package no.nav.opptjening.skatt.api;

import no.nav.opptjening.skatt.exceptions.MissingSekvensnummerException;
import no.nav.opptjening.skatt.exceptions.UnmappedException;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.mock.http.client.MockClientHttpResponse;
import org.springframework.web.client.HttpClientErrorException;

import static org.junit.Assert.fail;

public class SkattErrorHandlerTest {
    @Test
    public void invalidJson() throws Exception {
        String responseBody = "Her tryna det skikkelig";
        HttpStatus httpStatus = HttpStatus.BAD_REQUEST;

        ClientHttpResponse response = new MockClientHttpResponse(responseBody.getBytes(), httpStatus);

        SkattErrorHandler errorHandler = new SkattErrorHandler();

        try {
            errorHandler.handleError(response);
            fail("Expected an HttpClientErrorException to be thrown");
        } catch (HttpClientErrorException e) {
            // ok
        }
    }

    @Test
    public void validJsonButUnexpectedDataStructure() throws Exception {
        String responseBody = "{\"message\": \"Denne kan ikke mappes til en FeilmeldingDto!\"}";
        HttpStatus httpStatus = HttpStatus.BAD_REQUEST;

        ClientHttpResponse response = new MockClientHttpResponse(responseBody.getBytes(), httpStatus);

        SkattErrorHandler errorHandler = new SkattErrorHandler();

        try {
            errorHandler.handleError(response);
            fail("Expected an HttpClientErrorException to be thrown");
        } catch (HttpClientErrorException e) {
            // ok
        }
    }

    @Test
    public void validJsonButUnexpectedErrorCode() throws Exception {
        String responseBody = "{\"kode\":\"ZZ-000\", \"melding\": \"Her tryna det skikkelig\"}";
        HttpStatus httpStatus = HttpStatus.BAD_REQUEST;

        ClientHttpResponse response = new MockClientHttpResponse(responseBody.getBytes(), httpStatus);

        SkattErrorHandler errorHandler = new SkattErrorHandler();

        try {
            errorHandler.handleError(response);
            fail("Expected an UnmappedException to be thrown");
        } catch (UnmappedException e) {
            // ok
        }
    }

    @Test
    public void validJsonAndErrorCode() throws Exception {
        String responseBody = "{\"kode\": \"FA-001\", \"melding\": \"fraSekvensnummer må være satt\"}";
        HttpStatus httpStatus = HttpStatus.BAD_REQUEST;

        ClientHttpResponse response = new MockClientHttpResponse(responseBody.getBytes(), httpStatus);

        SkattErrorHandler errorHandler = new SkattErrorHandler();

        try {
            errorHandler.handleError(response);
            fail("Expected an MissingSekvensnummerException to be thrown");
        } catch (MissingSekvensnummerException e) {
            // ok
        }
    }
}