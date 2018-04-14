package no.nav.opptjening.hoi.hendelser;

import no.nav.opptjening.schema.PensjonsgivendeInntekt;
import no.nav.opptjening.schema.skatteetaten.BeregnetSkatt;
import no.nav.opptjening.schema.skatteetaten.hendelsesliste.Hendelse;
import no.nav.opptjening.skatt.api.beregnetskatt.BeregnetskattClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.metrics.CounterService;
import org.springframework.boot.actuate.metrics.GaugeService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Collections;

@Component
public class InntektHendelseConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(InntektHendelseConsumer.class);
    private final Consumer<String, Hendelse> hendelseConsumer;
    private final Producer<String, PensjonsgivendeInntekt> inntektsProducer;
    private final CounterService counterService;
    private final GaugeService gaugeService;

    private BeregnetskattClient beregnetskattClient;

    private PensjonsgivendeInntektMapper pensjonsgivendeInntektMapper = new PensjonsgivendeInntektMapper();

    public InntektHendelseConsumer(BeregnetskattClient beregnetskattClient, Consumer<String, Hendelse> hendelseConsumer, Producer<String, PensjonsgivendeInntekt> inntektProducer,
                                   CounterService counterService, GaugeService gaugeService) {
        this.beregnetskattClient = beregnetskattClient;
        this.hendelseConsumer = hendelseConsumer;
        this.inntektsProducer = inntektProducer;
        this.counterService = counterService;
        this.gaugeService = gaugeService;

        this.gaugeService.submit("inntektshendelser.current_sekvensnummer", 0);
        this.counterService.reset("inntektshendelser.received");
        this.counterService.reset("inntektshendelser.processed");

        hendelseConsumer.subscribe(Collections.singletonList(KafkaConfiguration.BEREGNET_SKATT_HENDELSE_TOPIC), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                LOG.info("Partition revoked: {}", partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                LOG.info("Partitions assigned: {}", partitions);
            }
        });
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 5000)
    public void poll() {
        ConsumerRecords<String, Hendelse> hendelser = hendelseConsumer.poll(500);

        for (int i = 0; i < hendelser.count(); i++) {
            counterService.increment("inntektshendelser.received");
        }

        try {
            for (ConsumerRecord<String, Hendelse> record : hendelser) {
                Hendelse hendelse = record.value();

                gaugeService.submit("inntektshendelser.current_sekvensnummer", hendelse.getSekvensnummer());

                LOG.info("HOI haandterer hendelse={}", hendelse);

                BeregnetSkatt beregnetSkatt = beregnetskattClient.getBeregnetSkatt("nav", hendelse.getGjelderPeriode(), hendelse.getIdentifikator());
                LOG.info("HOI sender inntekt='{}'", beregnetSkatt);
                inntektsProducer.send(new ProducerRecord<>(KafkaConfiguration.PENSJONSGIVENDE_INNTEKT_TOPIC, pensjonsgivendeInntektMapper.toPensjonsgivendeInntekt(beregnetSkatt)));
                counterService.increment("inntektshendelser.processed");
            }

            inntektsProducer.flush();
            hendelseConsumer.commitAsync();
        } catch (Exception e) {
            // TODO: skal vi avbryte eller fortsette prosessering? Hva gjør vi evt. med de nåværende-uncommitted?
            LOG.error("Error during processing of Hendelse/Inntekt", e);
        }
    }
}
