package no.nav.opptjening.dto;

public class InntektDto {

    public String personindentfikator;

    public String inntektsaar;

    public double pensjonsgivendeInntekt;

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
