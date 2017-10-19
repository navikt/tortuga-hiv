package no.nav.opptjening.skatt.exceptions;

public class IllegalAntallException extends BadRequestException {

    public IllegalAntallException(String message, Throwable cause) {
        super(message, cause);
    }
}
