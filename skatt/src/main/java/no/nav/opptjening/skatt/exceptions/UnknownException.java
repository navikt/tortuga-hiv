package no.nav.opptjening.skatt.exceptions;

public class UnknownException extends ApiException {
    public UnknownException(int httpStatus, String message, Throwable cause) {
        super(httpStatus, message, cause);
    }
}
