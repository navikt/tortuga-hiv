package no.nav.opptjening.skatt.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

class InntektBean {

    private String personindentfikator;

    private String inntektsaar;

    private double pensjonsgivendeInntekt;

    @JsonCreator
    InntektBean(@JsonProperty(value = "personindentfikator", required = true) String personindentfikator,
                      @JsonProperty(value = "inntektsaar", required = true) String inntektsaar,
                      @JsonProperty(value = "pensjonsgivendeInntekt", required =  true) double pensjonsgivendeInntekt) {
        this.personindentfikator = personindentfikator;
        this.inntektsaar = inntektsaar;
        this.pensjonsgivendeInntekt = pensjonsgivendeInntekt;
    }

    String getPersonindentfikator() {
        return personindentfikator;
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
                .append(personindentfikator)
                .append(", inntektsAar: ")
                .append(inntektsaar)
                .append(", pgi: ")
                .append(pensjonsgivendeInntekt)
                .append("]")
                .toString();
    }
}

