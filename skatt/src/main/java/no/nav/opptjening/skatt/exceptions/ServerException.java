package no.nav.opptjening.skatt.exceptions;

public class ServerException extends ApiException {

    public ServerException(int httpStatus, String message, Throwable cause) {
        super(httpStatus, message, cause);
    }
}
