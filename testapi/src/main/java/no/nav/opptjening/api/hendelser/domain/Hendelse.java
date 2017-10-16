package no.nav.opptjening.api.hendelser.domain;

import javax.persistence.*;

@Entity
@Table(name = "HENDELSER")
public class Hendelse {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public int sekvensnummer;

    @Column(nullable = false)
    public String identifikator;

    @Column(nullable = false)
    public String gjelderPeriode;

    protected Hendelse() {
    }

    public Hendelse(String fnr, String gjelderPeriode) {
        this.identifikator = fnr;
        this.gjelderPeriode = gjelderPeriode;
    }

    public int getSekvensnummer() {
        return sekvensnummer;
    }

    public String getIdentifikator() {
        return identifikator;
    }

    public String getGjelderPeriode() {
        return gjelderPeriode;
    }

    public String toString() {
        return String.format(
                "[sekvensnummer=%d, fnr=%s, gjelderPeriode=%s]",
                sekvensnummer, identifikator, gjelderPeriode);
    }
}
