package no.nav.opptjening.skatt.exceptions;

public class DatabaseUnavailableException extends InternalServerErrorException {

    public DatabaseUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
