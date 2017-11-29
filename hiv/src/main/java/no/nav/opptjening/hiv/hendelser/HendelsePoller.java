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

    @Value("${hiv.hendelser-per-request:1000}")
    private int maxHendelserPerRequest;

    public HendelsePoller(Hendelser inntektHendelser, Producer<String, Hendelse> hendelseProducer,
                          SekvensnummerStorage sekvensnummerStorage, CounterService counterService) {
        this.inntektHendelser = inntektHendelser;

        this.hendelseProducer = hendelseProducer;
        this.sekvensnummerStorage = sekvensnummerStorage;
        this.counterService = counterService;
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 5000)
    private void poll() {
        try {
            long nextSekvensnummer;
            try {
                nextSekvensnummer = sekvensnummerStorage.getSekvensnummer();
            } catch (NoOffsetForPartitionException e) {
                LOG.error("First run for consumer, setting sekvensnummer to 1", e);

                // TODO: there must be a better way of doing this
                nextSekvensnummer = 1;
            } catch (IllegalStateException e) {
                LOG.error(e.getMessage());
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

        for (HendelseDto hendelse : hendelser) {
            counterService.increment("hendelser.processed");
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
