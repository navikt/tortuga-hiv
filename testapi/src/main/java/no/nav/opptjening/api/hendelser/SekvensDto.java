package no.nav.opptjening.api.hendelser;

class SekvensDto {

    private long sekvensnummer;

    SekvensDto(long sekvensnummer) {
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
