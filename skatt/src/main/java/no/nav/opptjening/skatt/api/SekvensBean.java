package no.nav.opptjening.skatt.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

class SekvensBean {

    private long sekvensnummer;

    @JsonCreator
    SekvensBean(@JsonProperty(value = "sekvensnummer", required = true) long sekvensnummer) {
        this.sekvensnummer = sekvensnummer;
    }

    long getSekvensnummer() {
        return sekvensnummer;
    }

    @Override
    public String toString() {
        return String.format("[sekvensnummer=%d]", sekvensnummer);
    }
}
