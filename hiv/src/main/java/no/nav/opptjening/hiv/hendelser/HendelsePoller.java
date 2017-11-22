package no.nav.opptjening.hiv.hendelser;

import no.nav.opptjening.schema.Hendelse;
import no.nav.opptjening.skatt.api.hendelser.Hendelser;
import no.nav.opptjening.skatt.exceptions.EmptyResultException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class HendelsePoller {

    private static final Logger LOG = LoggerFactory.getLogger(HendelsePoller.class);

    private final Hendelser inntektHendelser;

    private final KafkaTemplate<String, Hendelse> kafkaTemplate;

    private final KafkaTemplate<String, Long> kafkaOffsetTemplate;

    @Value("${hiv.hendelser-per-request:1000}")
    private int maxHendelserPerRequest;

    /* TODO: read and write this number on a Kafka queue to manage offsets */
    private long nextSekvensnummer = 1;

    public HendelsePoller(Hendelser inntektHendelser, KafkaTemplate<String, Hendelse> kafkaTemplate,
                          KafkaTemplate<String, Long> kafkaOffsetTemplate) {
        this.inntektHendelser = inntektHendelser;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaOffsetTemplate = kafkaOffsetTemplate;
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 5000)
    public void poll() {
        try {
            LOG.info("Ser etter nye hendelser fra sekvensnummer = {}", nextSekvensnummer);

            inntektHendelser.getHendelser(nextSekvensnummer, maxHendelserPerRequest).stream()
                    .map(hendelseDto -> Hendelse.newBuilder()
                            .setSekvensnummer(hendelseDto.getSekvensnummer())
                            .setIdentifikator(hendelseDto.getIdentifikator())
                            .setGjelderPeriode(hendelseDto.getGjelderPeriode())
                            .build())
                    .forEach(this::skrivHendelse);
        } catch (EmptyResultException e) {
            /* do nothing */
        } catch (Exception e) {
            LOG.error("Feil ved polling av hendelser", e);
            /*
                throws                          when
                ResponseMappingException        Kan ikke mappe et OK JSON-resultat til HendelseDto
                RuntimeException                IOException
                UnmappableException             Kunne ikke mappe error-respons til FeilmeldingDto
                UnknownException                Kunne ikke mappe FeilmeldingDto til ApiException pga ukjent feilkode
                ClientException                 code = 4xx
                ServerException                 code = 5xx
                subklasse av ApiException       vi har mappet en FeilmeldingDto til ApiException
             */
        }
    }

    private void skrivHendelse(Hendelse hendelse) {
        LOG.info("varlser hendelse='{}'", hendelse);
        kafkaOffsetTemplate.send("tortuga.inntektshendelser.offsets", 0,"offset", hendelse.getSekvensnummer());
        nextSekvensnummer = hendelse.getSekvensnummer() + 1;

        kafkaTemplate.send("tortuga.inntektshendelser", hendelse);
    }
}
