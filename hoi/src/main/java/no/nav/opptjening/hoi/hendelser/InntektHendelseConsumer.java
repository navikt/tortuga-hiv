package no.nav.opptjening.hoi.hendelser;

import no.nav.opptjening.dto.InntektDto;
import no.nav.opptjening.dto.InntektKafkaHendelseDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class InntektHendelseConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(InntektHendelseConsumer.class);

    private String pgiEndepunkt;

    private RestTemplate restTemplate = new RestTemplate();

    public InntektHendelseConsumer(@Value("${skatt.pgi.endpoint}") String pgiEndepunkt) {
        this.pgiEndepunkt = pgiEndepunkt;
    }

    @KafkaListener(topics = "tortuga.inntektshendelser")
    public void mottaHendelse(InntektKafkaHendelseDto hendelse) {
        LOG.info("HOI haandterer hendelse='{}', api='{}'", hendelse, pgiEndepunkt);

        InntektDto inntekt = restTemplate.getForObject(pgiEndepunkt + "/" + hendelse.inntektsaar + "/" + hendelse.personindentfikator, InntektDto.class);

        LOG.info("HOI sender inntekt='{}'", inntekt);
    }
}
