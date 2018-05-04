package no.nav.opptjening.skatt.api.hendelseliste.exceptions;

import no.nav.opptjening.skatt.exceptions.BadRequestException;

public class MissingSekvensnummerException extends BadRequestException {

    public MissingSekvensnummerException(String message, Throwable cause) {
        super(message, cause);
    }
}
