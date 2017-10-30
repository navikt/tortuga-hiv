package no.nav.opptjening.skatt.exceptions;

public class InternalServerErrorException extends ServerException {

    public InternalServerErrorException(String message, Throwable cause) {
        super(500, message, cause);
    }
}
