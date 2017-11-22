package no.nav.opptjening.skatt.api;

public class FeilmeldingDto {

    private String kode;

    private String melding;

    public FeilmeldingDto() {}

    public String getKode() {
        return kode;
    }

    public void setKode(String kode) {
        this.kode = kode;
    }

    public String getMelding() {
        return melding;
    }

    public void setMelding(String melding) {
        this.melding = melding;
    }

    @Override
    public String toString() {
        return String.format("[kode=%s, melding=%s]", kode, melding);
    }
}
