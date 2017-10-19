package no.nav.opptjening.skatt.api;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.nav.opptjening.skatt.ExceptionMapper;
import no.nav.opptjening.skatt.dto.FeilmeldingDto;
import no.nav.opptjening.skatt.exceptions.ApiException;
import no.nav.opptjening.skatt.exceptions.UnmappedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import java.io.IOException;

public class SkattErrorHandler extends DefaultResponseErrorHandler {

    private ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger LOG = LoggerFactory.getLogger(SkattErrorHandler.class);

    private ExceptionMapper exceptionMapper = new ExceptionMapper();

    @Override
    public void handleError(ClientHttpResponse response) throws ApiException, IOException {
        try {
            super.handleError(response);
        } catch (HttpClientErrorException | HttpServerErrorException e) {
            try {
                FeilmeldingDto feilmelding = objectMapper.readValue(e.getResponseBodyAsString(), FeilmeldingDto.class);
                ApiException ex = exceptionMapper.mapException(feilmelding, e);

                if (ex == null) {
                    throw new UnmappedException(e.getStatusCode(), "Kunne ikke mappe FeilmeldingDto=" + feilmelding.toString() + " til ApiException pga ukjent feilkode", e);
                }

                throw ex;
            } catch (JsonParseException ex) {
                // respons er ikke gyldig json, så ingen vits å prøve å kaste til FeilmeldingDto
                // trenger vi logge noe her?
                //LOG.error("Respons er ikke gyldig JSON, kaster videre", ex);
                throw e;
            } catch (JsonMappingException ex) {
                // vi kan ikke kaste json til feilmelding dto; har skatteetaten oppdatert api-et sitt
                // til å svare med et annet format enn hva vi forventer? Er vår implementasjon av FeilmeldingDto feil?
                // Hvilken feil skal vi kaste videre? <e> eller <ex>?

                // trenger vi logge noe?
                //LOG.error("Kan ikke mappe til JSON datastruktur, kaster videre", ex);
                throw e;
            }
        }
    }
}
