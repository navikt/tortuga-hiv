package no.nav.opptjening.skatt.dto;

public class HendelseDto {

    private long sekvensnummer;

    private String identifikator;

    private String gjelderPeriode;

    public HendelseDto() {
    }

    public HendelseDto(long sekvensnummer, String identifikator, String gjelderPeriode) {
        this.sekvensnummer = sekvensnummer;
        this.identifikator = identifikator;
        this.gjelderPeriode = gjelderPeriode;
    }

    public long getSekvensnummer() {
        return sekvensnummer;
    }

    public String getIdentifikator() {
        return identifikator;
    }

    public String getGjelderPeriode() {
        return gjelderPeriode;
    }

    public void setSekvensnummer(long sekvensnummer) {
        this.sekvensnummer = sekvensnummer;
    }

    public void setIdentifikator(String identifikator) {
        this.identifikator = identifikator;
    }

    public void setGjelderPeriode(String gjelderPeriode) {
        this.gjelderPeriode = gjelderPeriode;
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
