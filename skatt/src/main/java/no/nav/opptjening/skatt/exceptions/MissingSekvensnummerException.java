package no.nav.opptjening.skatt.exceptions;

public class MissingSekvensnummerException extends BadRequestException {

    public MissingSekvensnummerException(String message, Throwable cause) {
        super(message, cause);
    }
}