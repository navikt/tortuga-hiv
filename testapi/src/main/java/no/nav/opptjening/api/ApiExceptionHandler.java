package no.nav.opptjening.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class ApiExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<ErrorResponse> exceptionHandler(ApiException ex) {
        LOG.info("HttpStatus: {}, melding: {}", ex.getHttpStatus(), ex.getMessage());

        MultiValueMap<String, String> headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");

        return new ResponseEntity<ErrorResponse>(new ErrorResponse(ex), headers, ex.getHttpStatus());
    }
}
