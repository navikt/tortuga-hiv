package no.nav.opptjening.skatt.api;

import org.springframework.web.client.RestTemplate;

public class InntektHendelser extends Hendelser {

    public InntektHendelser(String endepunkt, RestTemplate restTemplate) {
        super(endepunkt, restTemplate);
    }
}
