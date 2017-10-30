package no.nav.opptjening.skatt.exceptions;

public class ClientException extends ApiException {

    public ClientException(int httpStatus, String message, Throwable cause) {
        super(httpStatus, message, cause);
    }
}
