package no.nav.opptjening.api.pgi.domain;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name = "INNTEKTER")
public class Inntekt {

    @EmbeddedId
    private PersonInntekt pk;

    @Column(nullable = false)
    private double pensjonsgivendeInntekt;

    protected Inntekt() {
    }

    public Inntekt(PersonInntekt pk, double pensjonsgivendeInntekt) {
        this.pk = pk;
        this.pensjonsgivendeInntekt = pensjonsgivendeInntekt;
    }

    public String getIdentifikator() {
        return pk.identifikator;
    }

    public String getInntektsaar() {
        return pk.inntektsaar;
    }

    public double getPensjonsgivendeInntekt() {
        return pensjonsgivendeInntekt;
    }

    public String toString() {
        return String.format(
                "[identifikator=%d, inntektsaar=%s, pensjonsgivendeInntekt=%s]",
                pk.identifikator, pk.inntektsaar, pensjonsgivendeInntekt);
    }

    @Embeddable
    public static class PersonInntekt implements Serializable {
        @Column(nullable = false)
        private String identifikator;

        @Column(nullable = false)
        private String inntektsaar;

        public PersonInntekt(String identifikator, String inntektsaar) {
            this.identifikator = identifikator;
            this.inntektsaar = inntektsaar;
        }

        public String getIdentifikator() {
            return identifikator;
        }

        public String getInntektsaar() {
            return inntektsaar;
        }
    }
}
