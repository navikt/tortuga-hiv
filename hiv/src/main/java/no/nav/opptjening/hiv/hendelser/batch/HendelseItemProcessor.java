package no.nav.opptjening.hiv.hendelser.batch;

import no.nav.opptjening.schema.InntektHendelse;
import no.nav.opptjening.skatt.dto.HendelseDto;
import org.springframework.batch.item.ItemProcessor;

public class HendelseItemProcessor implements ItemProcessor<HendelseDto, InntektHendelse> {
    @Override
    public InntektHendelse process(HendelseDto hendelseDto) throws Exception {
        return InntektHendelse.newBuilder()
                .setIdentifikator(hendelseDto.getIdentifikator())
                .setGjelderPeriode(hendelseDto.getGjelderPeriode())
                .build();
    }
}
