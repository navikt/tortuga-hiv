package no.nav.opptjening.skatt.exceptions;

public abstract class ApiException extends IllegalArgumentException {

    private int httpStatus;

    protected ApiException(int httpStatus, String message, Throwable cause) {
        super(message, cause);
        this.httpStatus = httpStatus;
    }

    public int getHttpStatus() {
        return httpStatus;
    }
}
