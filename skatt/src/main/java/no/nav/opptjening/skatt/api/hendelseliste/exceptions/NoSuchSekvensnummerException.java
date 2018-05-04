package no.nav.opptjening.skatt.api.hendelseliste.exceptions;

import no.nav.opptjening.skatt.exceptions.NotFoundException;

public class NoSuchSekvensnummerException extends NotFoundException {

    public NoSuchSekvensnummerException(String message, Throwable cause) {
        super(message, cause);
    }
}
