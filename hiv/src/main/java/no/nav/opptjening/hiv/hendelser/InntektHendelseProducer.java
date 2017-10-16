package no.nav.opptjening.hiv.hendelser;

import no.nav.opptjening.skatt.api.InntektHendelser;
import no.nav.opptjening.skatt.dto.HendelseDto;
import no.nav.opptjening.skatt.dto.SekvensDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cglib.core.Local;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;

@Component
public class InntektHendelseProducer {

    private static final Logger LOG = LoggerFactory.getLogger(InntektHendelseProducer.class);

    private KafkaTemplate<String, InntektKafkaHendelseDto> kafkaTemplate;

    private InntektHendelser inntektHendelser;

    public InntektHendelseProducer(KafkaTemplate<String, InntektKafkaHendelseDto> kafkaTemplate, InntektHendelser inntektHendelser) {
        this.kafkaTemplate = kafkaTemplate;
        this.inntektHendelser = inntektHendelser;
    }

    public void sendHendelse(InntektKafkaHendelseDto hendelse) {
        LOG.info("varlser hendelse='{}'", hendelse);
        kafkaTemplate.send("tortuga.inntektshendelser", hendelse);
    }

    @Scheduled(initialDelay=5000, fixedRate = 20000)
    public void seEtterHendelser() {
        LOG.info("Ser etter nye hendelser");

        List<HendelseDto> hendelser = inntektHendelser.getHendelser();

        LOG.info("Fant {} hendelser", hendelser.size());

        for (HendelseDto hendelse : hendelser) {
            InntektKafkaHendelseDto kafkaHendelseDto = new InntektKafkaHendelseDto();
            kafkaHendelseDto.setSekvensnummer(hendelse.getSekvensnummer());
            kafkaHendelseDto.setIdentifikator(hendelse.getIdentifikator());
            kafkaHendelseDto.setGjelderPeriode(hendelse.getGjelderPeriode());

            sendHendelse(kafkaHendelseDto);
        }

        LocalDate dato = LocalDate.now();
        SekvensDto sisteSekvens = inntektHendelser.forsteSekvensEtter(dato);
        LOG.info("Siste sekvens etter {} = {}", dato, sisteSekvens);
    }
}