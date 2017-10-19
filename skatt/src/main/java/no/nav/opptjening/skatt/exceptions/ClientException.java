package no.nav.opptjening.skatt.exceptions;

import org.springframework.http.HttpStatus;

public class ClientException extends ApiException {

    public ClientException(HttpStatus httpStatus, String message, Throwable cause) {
        super(httpStatus, message, cause);
    }
}
