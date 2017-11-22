package no.nav.opptjening.skatt.api.pgi;

public class InntektDto {

    private String personindentfikator;

    private String inntektsaar;

    private double pensjonsgivendeInntekt;

    public InntektDto(String personindentfikator, String inntektsaar, double pensjonsgivendeInntekt) {
        this.personindentfikator = personindentfikator;
        this.inntektsaar = inntektsaar;
        this.pensjonsgivendeInntekt = pensjonsgivendeInntekt;
    }

    public String getPersonindentfikator() {
        return personindentfikator;
    }

    public String getInntektsaar() {
        return inntektsaar;
    }

    public double getPensjonsgivendeInntekt() {
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
