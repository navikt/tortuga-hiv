package no.nav.opptjening.skatt.exceptions;

import org.springframework.http.HttpStatus;

public class InternalServerErrorException extends ServerException {

    public InternalServerErrorException(String message, Throwable cause) {
        super(HttpStatus.INTERNAL_SERVER_ERROR, message, cause);
    }
}
