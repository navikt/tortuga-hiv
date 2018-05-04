package no.nav.opptjening.skatt.exceptions;

public abstract class HttpException extends IllegalArgumentException {

    private int httpStatus;

    protected HttpException(int httpStatus, String message, Throwable cause) {
        super(message, cause);
        this.httpStatus = httpStatus;
    }

    public int getHttpStatus() {
        return httpStatus;
    }
}
