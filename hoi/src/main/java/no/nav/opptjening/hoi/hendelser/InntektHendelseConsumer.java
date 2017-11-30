package no.nav.opptjening.hoi.hendelser;

import no.nav.opptjening.schema.Hendelse;
import no.nav.opptjening.schema.Inntekt;
import no.nav.opptjening.skatt.api.pgi.InntektDto;
import no.nav.opptjening.skatt.api.pgi.Inntekter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.metrics.CounterService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Collections;

@Component
public class InntektHendelseConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(InntektHendelseConsumer.class);
    private final Consumer<String, Hendelse> hendelseConsumer;
    private final Producer<String, Inntekt> inntektsProducer;
    private final CounterService counterService;

    private Inntekter inntekter;

    public InntektHendelseConsumer(Inntekter inntekter, Consumer<String, Hendelse> hendelseConsumer, Producer<String, Inntekt> inntektProducer,
                                   CounterService counterService) {
        this.inntekter = inntekter;
        this.hendelseConsumer = hendelseConsumer;
        this.inntektsProducer = inntektProducer;
        this.counterService = counterService;

        this.counterService.reset("inntektshendelser.received");
        this.counterService.reset("inntektshendelser.processed");
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 5000)
    public void poll() {
        hendelseConsumer.subscribe(Collections.singletonList("tortuga.inntektshendelser"));
        ConsumerRecords<String, Hendelse> hendelser = hendelseConsumer.poll(500);

        for (int i = 0; i < hendelser.count(); i++) {
            counterService.increment("inntektshendelser.received");
        }

        try {
            for (ConsumerRecord<String, Hendelse> record : hendelser) {
                Hendelse hendelse = record.value();

                LOG.info("HOI haandterer hendelse={}", hendelse);

                InntektDto inntekt = inntekter.hentInntekt(hendelse.getGjelderPeriode().toString(), hendelse.getIdentifikator().toString());
                LOG.info("HOI sender inntekt='{}'", inntekt);
                inntektsProducer.send(new ProducerRecord<>("tortuga.inntekter", Inntekt.newBuilder()
                        .setInntektsaar(inntekt.getInntektsaar())
                        .setIdentifikator(inntekt.getPersonindentfikator())
                        .setPensjonsgivendeInntekt(inntekt.getPensjonsgivendeInntekt())
                        .build()));
                counterService.increment("inntektshendelser.processed");
            }

            hendelseConsumer.commitAsync();
        } catch (Exception e) {
            // TODO: skal vi avbryte eller fortsette prosessering? Hva gjør vi evt. med de nåværende-uncommitted?
            LOG.error("Error during processing of Hendelse/Inntekt", e);
        }
    }
}
