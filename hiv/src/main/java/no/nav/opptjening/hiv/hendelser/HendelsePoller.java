package no.nav.opptjening.hiv.hendelser;

import no.nav.opptjening.schema.Hendelse;
import no.nav.opptjening.skatt.api.hendelser.HendelseDto;
import no.nav.opptjening.skatt.api.hendelser.Hendelser;
import no.nav.opptjening.skatt.exceptions.EmptyResultException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

@Component
public class HendelsePoller {

    private static final Logger LOG = LoggerFactory.getLogger(HendelsePoller.class);

    private final Hendelser inntektHendelser;

    private final Producer<String, Long> offsetProducer;
    private final Consumer<String, Long> offsetConsumer;
    private final Producer<String, Hendelse> hendelseProducer;
    private final TopicPartition offsetPartition;

    @Value("${hiv.hendelser-per-request:1000}")
    private int maxHendelserPerRequest;

    private boolean initialized;

    @Value("${hiv.initialize:false}")
    private boolean shouldInitialize;

    private ConsumerRecord<String, Long> currentSekvensnummerRecord = null;

    public HendelsePoller(Hendelser inntektHendelser, Producer<String, Hendelse> hendelseProducer,
                          Producer<String, Long> offsetProducer, Consumer<String, Long> offsetConsumer) {
        this.inntektHendelser = inntektHendelser;

        this.hendelseProducer = hendelseProducer;
        this.offsetProducer = offsetProducer;
        this.offsetConsumer = offsetConsumer;

        offsetPartition = new TopicPartition("tortuga.inntektshendelser.offsets", 0);
        offsetConsumer.assign(Collections.singletonList(offsetPartition));
    }

    private ConsumerRecord<String, Long> getNextSekvensnummer(Consumer<String, Long> consumer, TopicPartition partition) {
        LOG.info("Polling for messages, position = {}", consumer.position(partition));

        ConsumerRecords<String, Long> consumerRecords = consumer.poll(500);

        if (consumerRecords.isEmpty()) {
            throw new IllegalStateException("Poll returned zero elements");
        }

        LOG.info("Got {} messages", consumerRecords.count());
        List<ConsumerRecord<String, Long>> records = consumerRecords.records(partition);
        int count = records.size();

        ConsumerRecord<String, Long> last = records.get(count - 1);

        LOG.info("Poll returned: offset = {}, value = {}", last.offset(), last.value());

        return last;
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 5000)
    private void poll() {
        try {
            // TODO: remove initialization
            if (shouldInitialize && !initialized) {
                LOG.info("Initializing with sekvensnummer=1");

                try {
                    Future<RecordMetadata> meta = offsetProducer.send(new ProducerRecord<>(offsetPartition.topic(),
                            offsetPartition.partition(), "offset", 1L));
                    LOG.info("Flushing OffsetProducer");
                    offsetProducer.flush();

                    LOG.info("done: = {}, cancelled = {}, record = {}, offset = {}, value = {}", meta.isDone(), meta.isCancelled(), meta.get(), meta.get().offset());
                    initialized = true;
                } catch (Exception e) {
                    LOG.error("Error while initializing topic", e);
                }
            }

            try {
                // poll only when we have committed something
                if (currentSekvensnummerRecord == null) {
                    LOG.info("Polling for new offset record");
                    currentSekvensnummerRecord = getNextSekvensnummer(offsetConsumer, offsetPartition);
                }
            } catch (IllegalStateException e) {
                LOG.error(e.getMessage());
                return;
            }

            try {
                long lastSentSekvensnummer = handleSekvensnummer(currentSekvensnummerRecord.value());

                // we have now produced x messages from [nextSekvensnummer, nextSekvensnummer + x>
                offsetProducer.send(new ProducerRecord<>(offsetPartition.topic(), offsetPartition.partition(),
                        "offset", lastSentSekvensnummer + 1));
                offsetConsumer.commitAsync();

                currentSekvensnummerRecord = null;
            } catch (EmptyResultException e) {
                LOG.info("Empty result, waiting before trying again");
            }
        } catch (Exception e) {
            LOG.error("Uh oh", e);

            throw e;
        }
    }

    private long handleSekvensnummer(long sekvensnummer) {
        List<HendelseDto> hendelser = inntektHendelser.getHendelser(sekvensnummer, maxHendelserPerRequest);

        for (HendelseDto hendelse : hendelser) {
            hendelseProducer.send(new ProducerRecord<>("tortuga.inntektshendelser", null, Hendelse.newBuilder()
                    .setSekvensnummer(hendelse.getSekvensnummer())
                    .setIdentifikator(hendelse.getIdentifikator())
                    .setGjelderPeriode(hendelse.getGjelderPeriode())
                    .build()));
        }

        LOG.info("Flushing HendelseProducer");
        hendelseProducer.flush();

        // TODO: assume latest entry is largest sekvensnummer?
        return hendelser.get(hendelser.size() - 1).getSekvensnummer();
    }
}
