package no.nav.opptjening.skatt.exceptions;

public class UnmappableException extends ApiException {
    public UnmappableException(int httpStatus, String message, Throwable cause) {
        super(httpStatus, message, cause);
    }
}
