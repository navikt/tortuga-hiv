package no.nav.opptjening.skatt.exceptions;

import org.springframework.http.HttpStatus;

public class NotFoundException extends ClientException {

    public NotFoundException(String message, Throwable cause) {
        super(HttpStatus.NOT_FOUND, message, cause);
    }
}
