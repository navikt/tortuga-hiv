package no.nav.opptjening.api.hendelser;

import no.nav.opptjening.dto.InntektHendelseDto;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

class InntektHendelseFactory {

    private static final Random random = new Random();

    private static final int MIN_ITEMS = 0;

    private static final int MAX_ITEMS = 10;

    private static String lagTilfeldigPersonnr() {
        StringBuilder sb = new StringBuilder();

        int dag = 1 + random.nextInt(31);
        if (dag < 10) {
            sb.append("0");
        }
        sb.append(dag);

        int måned = 1 + random.nextInt(12);
        if (måned < 10) {
            sb.append("0");
        }
        sb.append(måned);

        // tilfeldig år mellom [00, 99]
        int år = random.nextInt(100);
        if (år < 10) {
            sb.append("0");
        }
        sb.append(år);

        return sb.append("12345").toString();
    }

    static InntektHendelseDto create() {
        InntektHendelseDto varsel = new InntektHendelseDto();

        varsel.endret = "2016-01-01 00:00:00";
        varsel.inntektsaar = "2017";
        varsel.personindentfikator = lagTilfeldigPersonnr();

        return varsel;
    }

    static List<InntektHendelseDto> createList(Date etterDato, int num) {
        List<InntektHendelseDto> liste = new ArrayList<>(num);

        for (int i = 0; i < num; i++) {
            liste.add(create());
        }

        return liste;
    }

    static List<InntektHendelseDto> createList(Date etterDato) {
        return createList(etterDato, MIN_ITEMS + random.nextInt(MAX_ITEMS - MIN_ITEMS + 1));
    }
}
