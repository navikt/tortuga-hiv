package no.nav.opptjening.skatt.api.hendelser;

public class HendelseDto {

    private long sekvensnummer;

    private String identifikator;

    private String gjelderPeriode;

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
