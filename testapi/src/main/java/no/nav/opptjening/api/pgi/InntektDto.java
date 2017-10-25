package no.nav.opptjening.api.pgi;

class InntektDto {

    private String personindentifikator;

    private String inntektsaar;

    private double pensjonsgivendeInntekt;

    InntektDto(String personindentifikator, String inntektsaar, double pensjonsgivendeInntekt) {
        this.personindentifikator = personindentifikator;
        this.inntektsaar = inntektsaar;
        this.pensjonsgivendeInntekt = pensjonsgivendeInntekt;
    }

    public String getPersonindentifikator() {
        return personindentifikator;
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
                .append(personindentifikator)
                .append(", inntektsAar: ")
                .append(inntektsaar)
                .append(", pgi: ")
                .append(pensjonsgivendeInntekt)
                .append("]")
                .toString();
    }
}

