package no.nav.opptjening.hoi.hendelser;

public class InntektKafkaHendelseDto {

    private String identifikator;

    private String gjelderPeriode;

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
        return sb.append("pid: ")
                .append(identifikator)
                .append(", gjelderPeriode: ")
                .append(gjelderPeriode)
                .append("]")
                .toString();
    }
}
