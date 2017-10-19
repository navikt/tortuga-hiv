package no.nav.opptjening.skatt.exceptions;

public class MissingInntektException extends BadRequestException {

    public MissingInntektException(String message, Throwable cause) {
        super(message, cause);
    }
}
