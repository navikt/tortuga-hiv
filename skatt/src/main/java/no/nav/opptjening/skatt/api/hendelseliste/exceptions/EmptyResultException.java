package no.nav.opptjening.skatt.api.hendelseliste.exceptions;

import no.nav.opptjening.skatt.exceptions.NotFoundException;

public class EmptyResultException extends NotFoundException {

    public EmptyResultException(String message, Throwable cause) {
        super(message, cause);
    }
}
