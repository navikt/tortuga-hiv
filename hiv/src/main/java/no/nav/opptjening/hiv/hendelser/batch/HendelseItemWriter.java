package no.nav.opptjening.hiv.hendelser.batch;

import no.nav.opptjening.schema.InntektHendelse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class HendelseItemWriter implements ItemWriter<InntektHendelse> {
    private static final Logger LOG = LoggerFactory.getLogger(HendelseItemWriter.class);

    private final KafkaTemplate<String, InntektHendelse> kafkaTemplate;

    public HendelseItemWriter(KafkaTemplate<String, InntektHendelse> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void write(List<? extends InntektHendelse> list) throws Exception {
        LOG.info("Skriver {} hendelser", list.size());

        for (InntektHendelse hendelse : list) {
            sendHendelse(hendelse);
        }
    }

    public void sendHendelse(InntektHendelse hendelse) {
        LOG.info("varlser hendelse='{}'", hendelse);
        kafkaTemplate.send("tortuga.inntektshendelser", hendelse);
    }
}
