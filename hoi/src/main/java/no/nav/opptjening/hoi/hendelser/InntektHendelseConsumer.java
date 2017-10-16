package no.nav.opptjening.hoi.hendelser;

import no.nav.opptjening.skatt.api.Inntekter;
import no.nav.opptjening.skatt.dto.InntektDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class InntektHendelseConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(InntektHendelseConsumer.class);

    private Inntekter inntekter;

    private RestTemplate restTemplate = new RestTemplate();

    public InntektHendelseConsumer(Inntekter inntekter) {
        this.inntekter = inntekter;
    }

    @KafkaListener(topics = "tortuga.inntektshendelser")
    public void mottaHendelse(InntektKafkaHendelseDto hendelse) {
        LOG.info("HOI haandterer hendelse={}", hendelse);

        InntektDto inntekt = inntekter.hentInntekt(hendelse.getGjelderPeriode(), hendelse.getIdentifikator());

        LOG.info("HOI sender inntekt='{}'", inntekt);
    }
}
