package no.nav.opptjening.skatt.exceptions;

public class ResponseMappingException extends ApiException {

    public ResponseMappingException(int httpStatus, String message, Throwable cause) {
        super(httpStatus, message, cause);
    }
}
