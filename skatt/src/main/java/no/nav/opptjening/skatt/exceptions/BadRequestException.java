package no.nav.opptjening.skatt.exceptions;

import org.springframework.http.HttpStatus;

public class BadRequestException extends ClientException {

    public BadRequestException(String message, Throwable cause) {
        super(HttpStatus.BAD_REQUEST, message, cause);
    }
}
