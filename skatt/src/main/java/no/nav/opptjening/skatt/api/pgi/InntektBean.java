package no.nav.opptjening.skatt.api.pgi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

class InntektBean {

    private String personindentifikator;

    private String inntektsaar;

    private double pensjonsgivendeInntekt;

    @JsonCreator
    InntektBean(@JsonProperty(value = "personindentifikator", required = true) String personindentifikator,
                      @JsonProperty(value = "inntektsaar", required = true) String inntektsaar,
                      @JsonProperty(value = "pensjonsgivendeInntekt", required =  true) double pensjonsgivendeInntekt) {
        this.personindentifikator = personindentifikator;
        this.inntektsaar = inntektsaar;
        this.pensjonsgivendeInntekt = pensjonsgivendeInntekt;
    }

    String getPersonindentifikator() {
        return personindentifikator;
    }

    String getInntektsaar() {
        return inntektsaar;
    }

    double getPensjonsgivendeInntekt() {
        return pensjonsgivendeInntekt;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        return sb.append("pid: ")
                .append(personindentifikator)
                .append(", inntektsAar: ")
                .append(inntektsaar)
                .append(", pgi: ")
                .append(pensjonsgivendeInntekt)
                .append("]")
                .toString();
    }
}

