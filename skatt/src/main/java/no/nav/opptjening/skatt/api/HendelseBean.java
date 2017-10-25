package no.nav.opptjening.skatt.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

class HendelseBean {

    private long sekvensnummer;

    private String identifikator;

    private String gjelderPeriode;

    @JsonCreator
    HendelseBean(@JsonProperty(value = "sekvensnummer", required = true) long sekvensnummer,
                       @JsonProperty(value = "identifikator", required = true) String identifikator,
                       @JsonProperty(value = "gjelderPeriode", required = true) String gjelderPeriode) {
        this.sekvensnummer = sekvensnummer;
        this.identifikator = identifikator;
        this.gjelderPeriode = gjelderPeriode;
    }

    long getSekvensnummer() {
        return sekvensnummer;
    }

    String getIdentifikator() {
        return identifikator;
    }

    String getGjelderPeriode() {
        return gjelderPeriode;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        return sb.append("sekvensnummer: ")
                .append(sekvensnummer)
                .append(", pid: ")
                .append(identifikator)
                .append(", gjelderPeriode: ")
                .append(gjelderPeriode)
                .append("]")
                .toString();
    }

}
