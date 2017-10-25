package no.nav.opptjening.skatt.api.pgi;

import no.nav.opptjening.skatt.api.hendelser.Hendelser;
import org.springframework.web.client.RestTemplate;

public class InntektHendelser extends Hendelser {

    public InntektHendelser(String endepunkt, RestTemplate restTemplate) {
        super(endepunkt, restTemplate);
    }
}
