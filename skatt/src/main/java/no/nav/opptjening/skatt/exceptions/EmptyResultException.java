package no.nav.opptjening.skatt.exceptions;

public class EmptyResultException extends NotFoundException {

    public EmptyResultException(String message, Throwable cause) {
        super(message, cause);
    }
}
