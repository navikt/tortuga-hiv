package no.nav.opptjening.skatt.api;

import no.nav.opptjening.skatt.dto.InntektDto;
import org.springframework.web.client.RestTemplate;

public class Inntekter {

    private RestTemplate restTemplate = new RestTemplate();

    private String endepunkt;

    public Inntekter(String endepunkt) {
        this.endepunkt = endepunkt;
    }

    public InntektDto hentInntekt(String inntektsaar, String personidentifikator) {
        return restTemplate.getForObject(endepunkt + "/{inntektsaar}/{pid}", InntektDto.class, inntektsaar, personidentifikator);
    }
}
