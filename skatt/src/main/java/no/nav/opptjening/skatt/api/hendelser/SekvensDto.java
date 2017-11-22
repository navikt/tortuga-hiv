package no.nav.opptjening.skatt.api.hendelser;

public class SekvensDto {

    private long sekvensnummer;

    public SekvensDto(long sekvensnummer) {
        this.sekvensnummer = sekvensnummer;
    }

    public long getSekvensnummer() {
        return sekvensnummer;
    }

    @Override
    public String toString() {
        return String.format("[sekvensnummer=%d]", sekvensnummer);
    }
}
