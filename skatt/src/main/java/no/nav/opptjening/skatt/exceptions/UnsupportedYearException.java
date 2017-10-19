package no.nav.opptjening.skatt.exceptions;

public class UnsupportedYearException extends BadRequestException {

    public UnsupportedYearException(String message, Throwable cause) {
        super(message, cause);
    }
}
