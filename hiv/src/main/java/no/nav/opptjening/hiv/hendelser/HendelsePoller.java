package no.nav.opptjening.hiv.hendelser;

import no.nav.opptjening.schema.Hendelse;
import no.nav.opptjening.skatt.api.hendelser.HendelseDto;
import no.nav.opptjening.skatt.api.hendelser.Hendelser;
import no.nav.opptjening.skatt.exceptions.EmptyResultException;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.metrics.CounterService;
import org.springframework.boot.actuate.metrics.GaugeService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class HendelsePoller {

    private static final Logger LOG = LoggerFactory.getLogger(HendelsePoller.class);

    private final Hendelser inntektHendelser;
    private final Producer<String, Hendelse> hendelseProducer;
    private final SekvensnummerStorage sekvensnummerStorage;
    private final CounterService counterService;
    private final GaugeService gaugeService;

    @Value("${hiv.hendelser-per-request:1000}")
    private int maxHendelserPerRequest;

    public HendelsePoller(Hendelser inntektHendelser, Producer<String, Hendelse> hendelseProducer,
                          SekvensnummerStorage sekvensnummerStorage, CounterService counterService, GaugeService gaugeService) {
        this.inntektHendelser = inntektHendelser;

        this.hendelseProducer = hendelseProducer;
        this.sekvensnummerStorage = sekvensnummerStorage;
        this.gaugeService = gaugeService;

        this.counterService = counterService;
        this.counterService.reset("hendelser.received");
        this.counterService.reset("hendelser.processed");
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 5000)
    private void poll() {
        try {
            long nextSekvensnummer;
            try {
                nextSekvensnummer = sekvensnummerStorage.getSekvensnummer();
                gaugeService.submit("hendelser.current_sekvensnummer", nextSekvensnummer);
            } catch (NoOffsetForPartitionException e) {
                LOG.warn("First run for consumer, setting sekvensnummer to 1", e);

                // TODO: there must be a better way of doing this
                sekvensnummerStorage.persistSekvensnummer(0);
                return;
            } catch (IllegalStateException e) {
                LOG.info(e.getMessage(), e);
                return;
            }

            try {
                long lastSentSekvensnummer = handleSekvensnummer(nextSekvensnummer);
                sekvensnummerStorage.persistSekvensnummer(lastSentSekvensnummer);
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

        for (int i = 0; i < hendelser.size(); i++) {
            counterService.increment("hendelser.received");
        }

        for (HendelseDto hendelse : hendelser) {
            counterService.increment("hendelser.processed");
            hendelseProducer.send(new ProducerRecord<>("tortuga.inntektshendelser", hendelse.getGjelderPeriode() + "-" + hendelse.getIdentifikator(), Hendelse.newBuilder()
                    .setSekvensnummer(hendelse.getSekvensnummer())
                    .setIdentifikator(hendelse.getIdentifikator())
                    .setGjelderPeriode(hendelse.getGjelderPeriode())
                    .build()));
        }

        LOG.debug("Flushing HendelseProducer");
        hendelseProducer.flush();

        // TODO: assume latest entry is largest sekvensnummer?
        return hendelser.get(hendelser.size() - 1).getSekvensnummer();
    }
}
