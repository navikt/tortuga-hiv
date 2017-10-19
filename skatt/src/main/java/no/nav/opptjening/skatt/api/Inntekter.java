package no.nav.opptjening.skatt.api;

import no.nav.opptjening.skatt.dto.InntektDto;
import org.springframework.web.client.RestTemplate;

public class Inntekter {

    private RestTemplate restTemplate;

    private String endepunkt;

    public Inntekter(String endepunkt, RestTemplate restTemplate) {
        this.endepunkt = endepunkt;
        this.restTemplate = restTemplate;
    }

    public InntektDto hentInntekt(String inntektsaar, String personidentifikator) {
        return restTemplate.getForObject(endepunkt + "/{inntektsaar}/{pid}", InntektDto.class, inntektsaar, personidentifikator);
    }
}
