package no.nav.opptjening.hiv.hendelser.batch;

import no.nav.opptjening.hiv.hendelser.support.BatchLoggEntry;
import no.nav.opptjening.hiv.hendelser.support.BatchLoggService;
import no.nav.opptjening.skatt.api.InntektHendelser;
import no.nav.opptjening.skatt.dto.HendelseDto;
import no.nav.opptjening.skatt.exceptions.ApiException;
import no.nav.opptjening.skatt.exceptions.EmptyResultException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@Component
public class HendelseItemReader implements ItemStreamReader<HendelseDto> {
    private static final Logger LOG = LoggerFactory.getLogger(HendelseItemReader.class);

    @Value("${hiv.hendelser-per-request:1000}")
    private int maxHendelserPerRequest;

    private final InntektHendelser inntektHendelser;

    private long nextSekvensnummer;

    private long lastWrittenSekvensnummer;

    private BatchLoggService service;

    private Iterator<HendelseDto> hendelseIterator;

    public HendelseItemReader(BatchLoggService service, InntektHendelser inntektHendelser) {
        this.service = service;
        this.inntektHendelser = inntektHendelser;
    }

    public void readFromApi() {
        LOG.info("Ser etter nye hendelser");

        try {
            List<HendelseDto> hendelser = inntektHendelser.getHendelser(nextSekvensnummer, maxHendelserPerRequest);
            hendelseIterator = hendelser.iterator();
            LOG.info("Fant {} hendelser", hendelser.size());
        } catch (EmptyResultException e) {
            hendelseIterator = Collections.emptyIterator();
            LOG.info("Fant 0 hendelser");
        }
    }

    @Override
    public HendelseDto read() throws UnexpectedInputException, ParseException, NonTransientResourceException {
        if (hendelseIterator == null || !hendelseIterator.hasNext()) {
            try {
                readFromApi();

                if (!hendelseIterator.hasNext()) {
                    return null;
                }
            } catch (ResourceAccessException e) {
                throw new NonTransientResourceException("Nettverksfeil ved oppkobling til API", e);
            } catch (HttpMessageNotReadableException e) {
                // skatteetaten har respondert med 2xx, men vi klarer ikke å mappe responsen til HendelseRespons
                // alvorlig feil, kan ikke mappe JSON til objekt:
                // skatteetaten har endret API-et f.eks.
                throw new ParseException("Kan ikke mappe OK JSON-resultat til datastruktur", e);
            } catch (ApiException e) {
                throw new UnexpectedInputException("API-feil", e);
            } catch (HttpServerErrorException | HttpClientErrorException e) {
                throw new UnexpectedInputException("4xx-feil eller 5xx-feil fra Skatteetaten som vi ikke klarer å gjøre om til ApiException pga forskjellig format eller fordi vi ikke har implementert den", e);
            }
        }

        HendelseDto hendelseDto = hendelseIterator.next();
        lastWrittenSekvensnummer = hendelseDto.getSekvensnummer();
        nextSekvensnummer = lastWrittenSekvensnummer + 1;
        return hendelseDto;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        BatchLoggEntry latestEntry = service.findLatest();

        long previousSekvensnummer = 0L;
        if (latestEntry == null) {
            LOG.info("Ingen sekvensnummer i DB. Er dette første kjøring?");
        } else {
            LOG.info("Latest = {}", latestEntry);
            LOG.info("Setting fra_sekvensnummer=<til_sekvensnummer> fra forrige kjøring");
            previousSekvensnummer = latestEntry.getTilSekvensnummer();
        }

        executionContext.put("fra_sekvensnummer", previousSekvensnummer);

        LOG.info("Setting nextSekvensnummer=1 + {}", previousSekvensnummer);
        nextSekvensnummer = previousSekvensnummer + 1;
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        if (lastWrittenSekvensnummer == 0) {
            return;
        }

        LOG.info("Updating execution context with til_sekvensnummer={}", lastWrittenSekvensnummer);
        executionContext.put("til_sekvensnummer", lastWrittenSekvensnummer);
    }

    @Override
    public void close() throws ItemStreamException {
    }
}
