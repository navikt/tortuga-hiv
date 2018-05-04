package no.nav.opptjening.skatt.api.exceptions;

import no.nav.opptjening.skatt.exceptions.HttpException;

public class UkjentFeilkodeException extends HttpException {
    public UkjentFeilkodeException(int httpStatus, String message, Throwable cause) {
        super(httpStatus, message, cause);
    }
}
