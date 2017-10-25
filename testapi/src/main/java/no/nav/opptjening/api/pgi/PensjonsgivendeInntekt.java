package no.nav.opptjening.api.pgi;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/formueinntekt/pensjonsgivendeinntekt/{inntektsaar}/{personidentifikator}")
public class PensjonsgivendeInntekt {

    private InntektService service;

    public PensjonsgivendeInntekt(InntektService service) {
        this.service = service;
    }

    @RequestMapping(method = RequestMethod.GET)
    public InntektDto hentInntekt(@PathVariable String inntektsaar, @PathVariable String personidentifikator) {
        return service.hentInntekt(personidentifikator, inntektsaar);
    }
}
