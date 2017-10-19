package no.nav.opptjening.skatt.exceptions;

public class MissingAntallException extends BadRequestException {

    public MissingAntallException(String message, Throwable cause) {
        super(message, cause);
    }
}
