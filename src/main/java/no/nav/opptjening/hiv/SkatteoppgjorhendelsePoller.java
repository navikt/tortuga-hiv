package no.nav.opptjening.hiv;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import no.nav.opptjening.hiv.sekvensnummer.Sekvensnummer;
import no.nav.opptjening.skatt.client.Hendelsesliste;
import no.nav.opptjening.skatt.client.api.hendelseliste.HendelserClient;
import no.nav.opptjening.skatt.client.exceptions.HttpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

class SkatteoppgjorhendelsePoller {

    private static final Logger LOG = LoggerFactory.getLogger(SkatteoppgjorhendelsePoller.class);

    private final int antallHendelserPerRequest;
    private final HendelserClient beregnetskattHendelserClient;
    private final no.nav.opptjening.hiv.sekvensnummer.Sekvensnummer sekvensnummer;


    SkatteoppgjorhendelsePoller(HendelserClient beregnetskattHendelserClient, Sekvensnummer sekvensnummer, int antallHendelserPerRequest) {
        this.beregnetskattHendelserClient = beregnetskattHendelserClient;
        this.antallHendelserPerRequest = antallHendelserPerRequest;
        this.sekvensnummer = sekvensnummer;
    }

    List<Hendelsesliste.Hendelse> poll() throws IOException {

        pollCounter.inc();

        try {
            var hendelser = fetchHendelserUntilLimit();

            if (endOfHendelser(hendelser)) {
                throw new EmptyResultException("We have reached the end of the hendelseliste");
            }

            antallHendelserHentetTotalt.inc(hendelser.size());
            incrementAntallHendelserMetricByYear(hendelser);

            sekvensnummer.updateProcessedSekvensnummer(lastSekvensnummer(hendelser));

            return hendelser;
        } catch (EmptyResultException e) {
            skatteetatenEmptyResultCounter.inc();
            throw e;
        } catch (HttpException e) {
            skatteetatenErrorCounter.inc();
            throw e;
        }
    }

    private List<Hendelsesliste.Hendelse> fetchHendelserUntilLimit() throws IOException {
        while (!sekvensnummer.reachedSekvensnummerLimit()) {
            var hendelser = beregnetskattHendelserClient.getHendelser(sekvensnummer.next(), antallHendelserPerRequest).getHendelser();
            LOG.info("Fetched {} hendelser", hendelser.size());
            if (hendelser.isEmpty()) {
                sekvensnummer.incrementSekvensnummer(antallHendelserPerRequest);
            } else {
                return hendelser;
            }
        }
        return Collections.emptyList();
    }


    private long lastSekvensnummer(List<Hendelsesliste.Hendelse> hendelser) {
        return hendelser.get(hendelser.size() - 1).getSekvensnummer();
    }

    private boolean endOfHendelser(List<Hendelsesliste.Hendelse> hendelser) {
        return hendelser.isEmpty() || sekvensnummer.reachedSekvensnummerLimit();
    }

    private void incrementAntallHendelserMetricByYear(List<Hendelsesliste.Hendelse> hendelser) {
        hendelser.forEach(hendelse -> antallHendelserHentet.labels(hendelse.getGjelderPeriode()).inc());
    }

    private static final Counter antallHendelserHentet = Counter.build()
            .name("hendelser_received")
            .labelNames("year")
            .help("Antall hendelser hentet.").register();

    private static final Counter pollCounter = Counter.build()
            .name("hendelser_poll_count")
            .help("Hvor mange ganger vi pollet etter hendelser").register();

    private static final Counter antallHendelserHentetTotalt = Counter.build()
            .name("hendelser_received_total")
            .help("Antall hendelser hentet totalt.").register();

    private static final Gauge skatteetatenEmptyResultCounter = Gauge.build()
            .name("skatteetaten_empty_result_count")
            .help("Hvor mange ganger vi fikk et tomt resultat tilbake fra Skatteetaten").register();

    private static final Counter skatteetatenErrorCounter = Counter.build()
            .name("skatteetaten_error_count")
            .help("Antall feil vi har fått fra Skatteetaten sitt API").register();
}
