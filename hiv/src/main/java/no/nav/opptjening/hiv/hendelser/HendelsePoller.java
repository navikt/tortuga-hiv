package no.nav.opptjening.hiv.hendelser;

import no.nav.opptjening.schema.Hendelse;
import no.nav.opptjening.schema.HendelseOffset;
import no.nav.opptjening.skatt.api.hendelser.Hendelser;
import no.nav.opptjening.skatt.exceptions.EmptyResultException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class HendelsePoller {

    private static final Logger LOG = LoggerFactory.getLogger(HendelsePoller.class);

    private final Hendelser inntektHendelser;

    private final ConsumerFactory<String, HendelseOffset> consumerFactory;
    private final KafkaTemplate<String, Hendelse> kafkaTemplate;
    private final KafkaTemplate<String, HendelseOffset> kafkaOffsetTemplate;

    @Value("${hiv.hendelser-per-request:1000}")
    private int maxHendelserPerRequest;

    /* TODO: read and write this number on a Kafka queue to manage offsets */
    private long nextSekvensnummer = 0;

    public HendelsePoller(Hendelser inntektHendelser, ConsumerFactory<String, HendelseOffset> consumerFactory, KafkaTemplate<String, Hendelse> kafkaTemplate, KafkaTemplate<String, HendelseOffset> kafkaOffsetTemplate) {
        this.inntektHendelser = inntektHendelser;
        this.consumerFactory = consumerFactory;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaOffsetTemplate = kafkaOffsetTemplate;
    }

    private long initializeSekvensnummer(String topic) {
        Consumer<String, HendelseOffset> consumer = consumerFactory.createConsumer();

        TopicPartition partition = new TopicPartition(topic, 0);
        consumer.assign(Collections.singletonList(partition));

        consumer.seekToEnd(Collections.singletonList(partition));

        long offset = consumer.position(partition);

        LOG.info("Current offset is {}", offset);

        if (offset == 0) {
            return 1;
        }

        consumer.seek(partition, offset - 2);

        offset = consumer.position(new TopicPartition(topic, 0));
        LOG.info("Offset after seek is {}", offset);

        ConsumerRecords<String, HendelseOffset> consumerRecords = consumer.poll(1000);

        offset = consumer.position(new TopicPartition(topic, 0));
        LOG.info("Offset after poll is {}", offset);

        Iterable<ConsumerRecord<String, HendelseOffset>> records = consumerRecords.records(topic);

        long sekvensnummer = 0;
        for (ConsumerRecord<String, HendelseOffset> rec : records) {
            LOG.info("Record offset={} key={} value={}", rec.offset(), rec.key(), rec.value().getSekvensnummer());

            sekvensnummer = rec.value().getSekvensnummer();
        }
        consumer.close();
        return sekvensnummer;
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 5000)
    public void poll() {

        if (nextSekvensnummer == 0) {
            nextSekvensnummer = initializeSekvensnummer("tortuga.inntektshendelser.offsets");
        }

        List<Hendelse> hendelser;

        try {
            LOG.info("Ser etter nye hendelser fra sekvensnummer = {}", nextSekvensnummer);

            hendelser = inntektHendelser.getHendelser(nextSekvensnummer, maxHendelserPerRequest).stream()
                    .map(hendelseDto -> Hendelse.newBuilder()
                            .setSekvensnummer(hendelseDto.getSekvensnummer())
                            .setIdentifikator(hendelseDto.getIdentifikator())
                            .setGjelderPeriode(hendelseDto.getGjelderPeriode())
                            .build())
                    .collect(Collectors.toList());

            sendHendelser(hendelser);
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

    @Transactional
    protected void sendHendelser(List<Hendelse> hendelser) {
        try {
            for (Hendelse hendelse : hendelser) {
                LOG.info("varslet hendelse={}", hendelse);
                kafkaTemplate.send("tortuga.inntektshendelser", 0, null, hendelse);
            }

            nextSekvensnummer = hendelser.get(hendelser.size() - 1).getSekvensnummer() + 1;
            kafkaOffsetTemplate.send("tortuga.inntektshendelser.offsets", HendelseOffset.newBuilder()
                    .setSekvensnummer(nextSekvensnummer).build());
        } catch (Exception e) {
            LOG.error("Feil ved sending av melding", e);
        }
    }
}
