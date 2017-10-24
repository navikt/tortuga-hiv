package no.nav.opptjening.hiv.hendelser.batch;

import no.nav.opptjening.hiv.hendelser.InntektKafkaHendelseDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class HendelseItemWriter implements ItemWriter<InntektKafkaHendelseDto> {
    private static final Logger LOG = LoggerFactory.getLogger(HendelseItemWriter.class);

    private final KafkaTemplate<String, InntektKafkaHendelseDto> kafkaTemplate;

    public HendelseItemWriter(KafkaTemplate<String, InntektKafkaHendelseDto> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void write(List<? extends InntektKafkaHendelseDto> list) throws Exception {
        LOG.info("Skriver {} hendelser", list.size());

        for (InntektKafkaHendelseDto hendelse : list) {
            sendHendelse(hendelse);
        }
    }

    public void sendHendelse(InntektKafkaHendelseDto hendelse) {
        LOG.info("varlser hendelse='{}'", hendelse);
        kafkaTemplate.send("tortuga.inntektshendelser", hendelse);
    }
}
