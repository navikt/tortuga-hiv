package no.nav.opptjening.skatt.exceptions;

public class IllegalSekvensnummerException extends BadRequestException {

    public IllegalSekvensnummerException(String message, Throwable cause) {
        super(message, cause);
    }
}
