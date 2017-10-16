package no.nav.opptjening.skatt.dto;

public class SekvensDto {

    private int sekvensnummer;

    public SekvensDto() {
    }

    public SekvensDto(int sekvensnummer) {
        this.sekvensnummer = sekvensnummer;
    }

    public int getSekvensnummer() {
        return sekvensnummer;
    }

    public void setSekvensnummer(int sekvensnummer) {
        this.sekvensnummer = sekvensnummer;
    }

    @Override
    public String toString() {
        return String.format("[sekvensnummer=%d]", sekvensnummer);
    }
}
