package no.nav.opptjening.hoi.hendelser;

import no.nav.opptjening.schema.InntektHendelse;
import no.nav.opptjening.skatt.api.pgi.Inntekter;
import no.nav.opptjening.skatt.dto.InntektDto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InntektHendelseConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(InntektHendelseConsumer.class);

    private Inntekter inntekter;

    public InntektHendelseConsumer(Inntekter inntekter) {
        this.inntekter = inntekter;
    }

    @KafkaListener(topics = "tortuga.inntektshendelser")
    public void mottaHendelse(ConsumerRecord<String, InntektHendelse> record) {
        InntektHendelse hendelse = record.value();
        LOG.info("HOI haandterer hendelse={}", hendelse);

        try {
            InntektDto inntekt = inntekter.hentInntekt(hendelse.getGjelderPeriode().toString(), hendelse.getIdentifikator().toString());
            LOG.info("HOI sender inntekt='{}'", inntekt);
        } catch (Exception e) {
            LOG.error("Her skjedde det noe galt ja", e);
        }
    }
}
