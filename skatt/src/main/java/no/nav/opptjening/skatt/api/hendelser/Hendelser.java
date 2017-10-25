package no.nav.opptjening.skatt.api.hendelser;

import no.nav.opptjening.skatt.dto.HendelseDto;
import no.nav.opptjening.skatt.dto.SekvensDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDate;
import java.util.List;

/**
 * Abstrahering av Hendelse API til Skatteetaten:
 * https://skatteetaten.github.io/datasamarbeid-api-dokumentasjon/reference_feed
 *
 * Endepunktet er dokumentert som:
 * https://<env>:<port>/api/<domene>/<ressurs>/hendelser
 *
 * For Pensjonsgivende inntekt blir endepunktet:
 * https://<env>:<port>/api/formueinntekt/beregnetskatt/hendelser,
 * hvor domene = formueinntekt og ressurs = beregnetskatt
 *
 * Skatteetaten har dokumentert følgende miljøer:
 * Akseptansetest (AT)  http://api-at.sits.no
 * Produksjon (Prod)    http://api.skatteetaten.no
 */
public class Hendelser {

    private static final Logger LOG = LoggerFactory.getLogger(Hendelser.class);

    private RestTemplate restTemplate;

    private String endepunkt;

    public Hendelser(String endepunkt, RestTemplate restTemplate) {
        this.endepunkt = endepunkt;
        this.restTemplate = restTemplate;
    }

    public SekvensDto forsteSekvensEtter(LocalDate dato) {
        SekvensBean sekvensBean = restTemplate.getForObject(endepunkt + "/start?dato={dato}", SekvensBean.class, dato.toString());
        return new SekvensDto(sekvensBean.getSekvensnummer());
    }

    public List<HendelseDto> getHendelser(long fraSekvens, int antall) {
        ResponseEntity<HendelseResponsBean> response = restTemplate.exchange(
                endepunkt + "?fraSekvensnummer={sekvens}&antall={antall}",
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<HendelseResponsBean>() {
                },
                fraSekvens,
                antall
        );
        return response.getBody().getHendelser();
    }
}
