package no.nav.opptjening.api.pgi;

import no.nav.opptjening.dto.InntektDto;

import java.time.Year;
import java.util.Random;

class InntektFactory {

    private static final Random random = new Random();

    private static final int MIN_YEAR = 1967;
    private static final int MAX_YEAR = Year.now().getValue();

    static InntektDto create(String personnr, String inntektsaar, double pgi) {
        InntektDto inntektDto = new InntektDto();
        inntektDto.personindentfikator = personnr;
        inntektDto.inntektsaar = inntektsaar;
        inntektDto.pensjonsgivendeInntekt = pgi;
        return inntektDto;
    }

    static InntektDto create(String personnr, String inntektsaar) {
        // tilfeldig inntekt mellom [0, 1 000 000]
        return create(personnr, inntektsaar, 1000000 * random.nextDouble());
    }

    static InntektDto create(String personnr) {
        // tilfeldig år mellom [1967, nå] (inclusive, inclusive)
        return create(personnr, String.valueOf(MIN_YEAR + random.nextInt(MAX_YEAR - MIN_YEAR + 1)));
    }

    static InntektDto create() {
        return create("12345678901");
    }
}