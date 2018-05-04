package no.nav.opptjening.skatt.api.hendelseliste.exceptions;

import no.nav.opptjening.skatt.exceptions.BadRequestException;

public class IllegalSekvensnummerException extends BadRequestException {

    public IllegalSekvensnummerException(String message, Throwable cause) {
        super(message, cause);
    }
}
