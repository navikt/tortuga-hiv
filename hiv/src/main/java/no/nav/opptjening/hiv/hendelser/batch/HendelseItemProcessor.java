package no.nav.opptjening.hiv.hendelser.batch;

import no.nav.opptjening.hiv.hendelser.InntektKafkaHendelseDto;
import no.nav.opptjening.skatt.dto.HendelseDto;
import org.springframework.batch.item.ItemProcessor;

public class HendelseItemProcessor implements ItemProcessor<HendelseDto, InntektKafkaHendelseDto> {
    @Override
    public InntektKafkaHendelseDto process(HendelseDto hendelseDto) throws Exception {
        Thread.sleep(500);

        InntektKafkaHendelseDto hendelseKafkaDto = new InntektKafkaHendelseDto();

        hendelseKafkaDto.setIdentifikator(hendelseDto.getIdentifikator());
        hendelseKafkaDto.setGjelderPeriode(hendelseDto.getGjelderPeriode());

        return hendelseKafkaDto;
    }
}
