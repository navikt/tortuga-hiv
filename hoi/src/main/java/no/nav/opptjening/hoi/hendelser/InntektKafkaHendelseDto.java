package no.nav.opptjening.hoi.hendelser;

public class InntektKafkaHendelseDto {

    private int sekvensnummer;

    private String identifikator;

    private String gjelderPeriode;

    public int getSekvensnummer() {
        return sekvensnummer;
    }

    public void setSekvensnummer(int sekvensnummer) {
        this.sekvensnummer = sekvensnummer;
    }

    public String getIdentifikator() {
        return identifikator;
    }

    public void setIdentifikator(String identifikator) {
        this.identifikator = identifikator;
    }

    public String getGjelderPeriode() {
        return gjelderPeriode;
    }

    public void setGjelderPeriode(String gjelderPeriode) {
        this.gjelderPeriode = gjelderPeriode;
    }

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
