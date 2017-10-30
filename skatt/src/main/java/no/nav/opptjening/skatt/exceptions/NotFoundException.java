package no.nav.opptjening.skatt.exceptions;

public class NotFoundException extends ClientException {

    public NotFoundException(String message, Throwable cause) {
        super(404, message, cause);
    }
}
