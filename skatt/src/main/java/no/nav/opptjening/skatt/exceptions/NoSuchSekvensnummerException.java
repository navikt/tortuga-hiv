package no.nav.opptjening.skatt.exceptions;

public class NoSuchSekvensnummerException extends NotFoundException {

    public NoSuchSekvensnummerException(String message, Throwable cause) {
        super(message, cause);
    }
}
