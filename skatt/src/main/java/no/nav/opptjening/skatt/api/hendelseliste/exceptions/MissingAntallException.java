package no.nav.opptjening.skatt.api.hendelseliste.exceptions;

import no.nav.opptjening.skatt.exceptions.BadRequestException;

public class MissingAntallException extends BadRequestException {

    public MissingAntallException(String message, Throwable cause) {
        super(message, cause);
    }
}
