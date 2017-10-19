package no.nav.opptjening.skatt.exceptions;

import org.springframework.http.HttpStatus;

public abstract class ApiException extends IllegalArgumentException {

    private HttpStatus httpStatus;

    protected ApiException(HttpStatus httpStatus, String message, Throwable cause) {
        super(message, cause);
        this.httpStatus = httpStatus;
    }

    public HttpStatus getHttpStatus() {
        return httpStatus;
    }
}
