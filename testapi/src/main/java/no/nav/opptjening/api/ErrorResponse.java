package no.nav.opptjening.api;

public class ErrorResponse {

    private ApiException exception;

    public ErrorResponse(ApiException e) {
        exception = e;
    }

    public String getKode() {
        return exception.getKode();
    }

    public String getMelding() {
        return exception.getMessage();
    }
}
