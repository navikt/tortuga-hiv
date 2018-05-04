package no.nav.opptjening.skatt.api.hendelseliste.exceptions;

import no.nav.opptjening.skatt.exceptions.BadRequestException;

public class IllegalAntallException extends BadRequestException {

    public IllegalAntallException(String message, Throwable cause) {
        super(message, cause);
    }
}
