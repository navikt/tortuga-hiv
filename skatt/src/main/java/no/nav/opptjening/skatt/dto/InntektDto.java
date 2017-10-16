package no.nav.opptjening.skatt.dto;

public class InntektDto {

    public String personindentfikator;

    public String inntektsaar;

    public double pensjonsgivendeInntekt;

    public InntektDto() {
    }

    public InntektDto(String personindentfikator, String inntektsaar, double pensjonsgivendeInntekt) {
        this.personindentfikator = personindentfikator;
        this.inntektsaar = inntektsaar;
        this.pensjonsgivendeInntekt = pensjonsgivendeInntekt;
    }

    public String getPersonindentfikator() {
        return personindentfikator;
    }

    public void setPersonindentfikator(String personindentfikator) {
        this.personindentfikator = personindentfikator;
    }

    public String getInntektsaar() {
        return inntektsaar;
    }

    public void setInntektsaar(String inntektsaar) {
        this.inntektsaar = inntektsaar;
    }

    public double getPensjonsgivendeInntekt() {
        return pensjonsgivendeInntekt;
    }

    public void setPensjonsgivendeInntekt(double pensjonsgivendeInntekt) {
        this.pensjonsgivendeInntekt = pensjonsgivendeInntekt;
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
