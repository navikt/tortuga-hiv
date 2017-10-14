package no.nav.opptjening.api.hendelser.domain;

import javax.persistence.*;

@Entity
@Table(name = "INNTEKTSFEED")
public class InntektHendelse {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public Long id;

    @Column(nullable = false)
    public String fnr;

    @Column(nullable = false)
    public String gjelderPeriode;

    @Column(nullable = false)
    public String dokumenttilstand;

    protected InntektHendelse() {
    }

    public InntektHendelse(String fnr, String gjelderPeriode, String dokumenttilstand) {
        this.fnr = fnr;
        this.gjelderPeriode = gjelderPeriode;
        this.dokumenttilstand = dokumenttilstand;
    }

    public String toString() {
        return String.format(
                "[id=%d, fnr=%s, gjelderPeriode=%s, dokumenttilstand=%s]",
                id, fnr, gjelderPeriode, dokumenttilstand);
    }
}
