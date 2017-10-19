package no.nav.opptjening.hiv.hendelser;

import no.nav.opptjening.skatt.api.InntektHendelser;
import no.nav.opptjening.skatt.dto.HendelseDto;
import no.nav.opptjening.skatt.dto.SekvensDto;
import no.nav.opptjening.skatt.exceptions.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;

import java.time.LocalDate;
import java.util.List;

@Component
public class InntektHendelseProducer {

    private static final Logger LOG = LoggerFactory.getLogger(InntektHendelseProducer.class);

    private KafkaTemplate<String, InntektKafkaHendelseDto> kafkaTemplate;

    private InntektHendelser inntektHendelser;

    private static final int MAX_HENDELSER_PER_REQUEST = 5;

    public InntektHendelseProducer(KafkaTemplate<String, InntektKafkaHendelseDto> kafkaTemplate, InntektHendelser inntektHendelser) {
        this.kafkaTemplate = kafkaTemplate;
        this.inntektHendelser = inntektHendelser;
    }

    public void sendHendelse(InntektKafkaHendelseDto hendelse) {
        LOG.info("varlser hendelse='{}'", hendelse);
        kafkaTemplate.send("tortuga.inntektshendelser", hendelse);
    }

    @Scheduled(initialDelay=5000, fixedRate = 20000)
    public void seEtterHendelser() {
        LOG.info("Ser etter nye hendelser");

        try {
            LocalDate dato = LocalDate.now();
            SekvensDto sisteSekvens = inntektHendelser.forsteSekvensEtter(dato);
            LOG.info("Siste sekvens etter {} = {}", dato, sisteSekvens);

            List<HendelseDto> hendelser = inntektHendelser.getHendelser(sisteSekvens.getSekvensnummer(), MAX_HENDELSER_PER_REQUEST);

            LOG.info("Fant {} hendelser", hendelser.size());

            for (HendelseDto hendelse : hendelser) {
                InntektKafkaHendelseDto kafkaHendelseDto = new InntektKafkaHendelseDto();
                kafkaHendelseDto.setSekvensnummer(hendelse.getSekvensnummer());
                kafkaHendelseDto.setIdentifikator(hendelse.getIdentifikator());
                kafkaHendelseDto.setGjelderPeriode(hendelse.getGjelderPeriode());

                sendHendelse(kafkaHendelseDto);
            }
        } catch (ResourceAccessException e) {
            LOG.error("Nettverksfeil ved oppkobling til API", e);
        } catch (HttpMessageNotReadableException e) {
            // skatteetaten har respondert med 2xx, men vi klarer ikke å mappe responsen til HendelseRespons
            // alvorlig feil, kan ikke mappe JSON til objekt:
            // skatteetaten har endret API-et f.eks.
            LOG.error("Kan ikke mappe OK JSON-resultat til datastruktur", e);
        } catch (ApiException e) {
            LOG.error("API-feil", e);
        } catch (HttpServerErrorException | HttpClientErrorException e) {
            LOG.error("4xx-feil eller 5xx-feil fra Skatteetaten som vi ikke klarer å gjøre om til ApiException pga forskjellig format eller fordi vi ikke har implementert den", e);
            LOG.debug("Responsen: {}", e.getResponseBodyAsString());
        }/* catch (NoSuchElementException e) {
            LOG.error("4xx-feil eller 5xx-feil fra Skatteetaten som vi ikke klarer å gjøre om til ApiException pga vi ikke har implementert den", e);
        }*/
    }
}