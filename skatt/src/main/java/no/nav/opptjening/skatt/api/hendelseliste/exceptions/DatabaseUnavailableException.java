package no.nav.opptjening.skatt.api.hendelseliste.exceptions;

import no.nav.opptjening.skatt.exceptions.InternalServerErrorException;

public class DatabaseUnavailableException extends InternalServerErrorException {

    public DatabaseUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
