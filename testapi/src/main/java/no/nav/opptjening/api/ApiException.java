package no.nav.opptjening.api;

import org.springframework.http.HttpStatus;

public class ApiException extends RuntimeException {

    private HttpStatus httpStatus;

    private String kode;

    public ApiException(HttpStatus httpStatus, String kode, String message) {
        super(message);
        this.httpStatus = httpStatus;
        this.kode = kode;
    }

    public HttpStatus getHttpStatus() {
        return httpStatus;
    }

    public String getKode() {
        return kode;
    }
}

