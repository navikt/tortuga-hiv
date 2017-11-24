package no.nav.opptjening.hiv.hendelser;

import no.nav.opptjening.schema.Hendelse;
import no.nav.opptjening.skatt.api.hendelser.Hendelser;
import no.nav.opptjening.skatt.exceptions.EmptyResultException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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

    private final ConsumerFactory<String, Long> consumerFactory;
    private final KafkaTemplate<String, Hendelse> kafkaTemplate;
    private final KafkaTemplate<String, Long> kafkaOffsetTemplate;

    private Consumer<String, Long> consumer;

    @Value("${hiv.hendelser-per-request:1000}")
    private int maxHendelserPerRequest;

    public HendelsePoller(Hendelser inntektHendelser, ConsumerFactory<String, Long> consumerFactory, KafkaTemplate<String, Hendelse> kafkaTemplate, KafkaTemplate<String, Long> kafkaOffsetTemplate) {
        this.inntektHendelser = inntektHendelser;
        this.consumerFactory = consumerFactory;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaOffsetTemplate = kafkaOffsetTemplate;
    }

    private long getNextSekvensnummer(Consumer<String, Long> consumer, String topic) {
        TopicPartition partition = new TopicPartition(topic, 0);
        consumer.assign(Collections.singletonList(partition));

        long offset = consumer.position(partition);
        LOG.info("Current offset is {}", offset);

        // seek to last committed offset, because poll() will continue
        // from the last polled offset (even though it's uncommitted)
        OffsetAndMetadata committed = consumer.committed(partition);
        if (committed == null) {
            // first-run?
            return 1;
        }

        consumer.seek(partition, committed.offset());
        LOG.info("Offset after seekToEnd is {}", offset);

        ConsumerRecords<String, Long> consumerRecords = consumer.poll(1000);

        offset = consumer.position(new TopicPartition(topic, 0));
        LOG.info("Offset after poll is {}", offset);

        Iterable<ConsumerRecord<String, Long>> records = consumerRecords.records(topic);

        long sekvensnummer = 1;
        for (ConsumerRecord<String, Long> rec : records) {
            LOG.info("Record offset={} key={} value={}", rec.offset(), rec.key(), rec.value());

            sekvensnummer = rec.value();
        }

        // TODO: assert that sekvensnummer != 1?

        return sekvensnummer;
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 5000)
    public void poll() {
        if (consumer == null) {
            consumer = consumerFactory.createConsumer();
        }

        long nextSekvensnummer = getNextSekvensnummer(consumer,"tortuga.inntektshendelser.offsets");

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
        } catch (EmptyResultException e) {
            /* do nothing */
            return;
        } catch (Exception e) {
            LOG.error("Feil ved polling av hendelser", e);
            return;
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

        try {
            sendHendelser(hendelser);

            nextSekvensnummer = hendelser.get(hendelser.size() - 1).getSekvensnummer() + 1;
            kafkaOffsetTemplate.send("tortuga.inntektshendelser.offsets", "offset", nextSekvensnummer);

            // TODO: what if kafkaOffsetTemplate hasn't completed sending?

            LOG.info("Committing consumer offset");
            consumer.commitSync();
        } catch (Exception e) {
            LOG.error("Feil ved sending av melding", e);
        }
    }

    @Transactional
    protected void sendHendelser(List<Hendelse> hendelser) {
        for (Hendelse hendelse : hendelser) {
            LOG.info("varslet hendelse={}", hendelse);
            kafkaTemplate.send("tortuga.inntektshendelser", 0, null, hendelse);
        }
    }
}
