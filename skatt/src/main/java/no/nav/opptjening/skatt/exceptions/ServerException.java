package no.nav.opptjening.skatt.exceptions;

import org.springframework.http.HttpStatus;

public class ServerException extends ApiException {

    public ServerException(HttpStatus httpStatus, String message, Throwable cause) {
        super(httpStatus, message, cause);
    }
}
