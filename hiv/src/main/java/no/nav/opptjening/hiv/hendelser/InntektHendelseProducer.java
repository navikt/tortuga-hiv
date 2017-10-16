package no.nav.opptjening.hiv.hendelser;

import no.nav.opptjening.skatt.dto.InntektHendelseDto;
import no.nav.opptjening.skatt.dto.InntektKafkaHendelseDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.List;

@Component
public class InntektHendelseProducer {

    private static final Logger LOG = LoggerFactory.getLogger(InntektHendelseProducer.class);

    private KafkaTemplate<String, InntektKafkaHendelseDto> kafkaTemplate;

    private String hendelseEndepunkt;

    private RestTemplate restTemplate = new RestTemplate();

    public InntektHendelseProducer(KafkaTemplate<String, InntektKafkaHendelseDto> kafkaTemplate, @Value("${skatt.hendelser.endpoint}") String hendelseEndepunkt) {
        this.kafkaTemplate = kafkaTemplate;
        this.hendelseEndepunkt = hendelseEndepunkt;
    }

    public void sendHendelse(InntektKafkaHendelseDto hendelse) {
        LOG.info("varlser hendelse='{}'", hendelse);
        kafkaTemplate.send("tortuga.inntektshendelser", hendelse);
    }

    @Scheduled(initialDelay=5000, fixedRate = 20000)
    public void seEtterHendelser() {
        LOG.info("Ser etter nye hendelser {}", hendelseEndepunkt);

        ResponseEntity<List<InntektHendelseDto>> response = restTemplate.exchange(hendelseEndepunkt,
                HttpMethod.GET, null, new ParameterizedTypeReference<List<InntektHendelseDto>>() {});

        List<InntektHendelseDto> hendelser = response.getBody();

        LOG.info("Fant {} hendelser", hendelser.size());

        for (InntektHendelseDto hendelse : hendelser) {
            InntektKafkaHendelseDto kafkaHendelseDto = new InntektKafkaHendelseDto();
            kafkaHendelseDto.endret = hendelse.endret;
            kafkaHendelseDto.personindentfikator = hendelse.personindentfikator;
            kafkaHendelseDto.inntektsaar = hendelse.inntektsaar;

            sendHendelse(kafkaHendelseDto);
        }
    }
}