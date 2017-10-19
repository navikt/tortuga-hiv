package no.nav.opptjening.skatt.exceptions;

import org.springframework.http.HttpStatus;

public class UnmappedException extends ApiException {
    public UnmappedException(HttpStatus httpStatus, String message, Throwable cause) {
        super(httpStatus, message, cause);
    }
}
