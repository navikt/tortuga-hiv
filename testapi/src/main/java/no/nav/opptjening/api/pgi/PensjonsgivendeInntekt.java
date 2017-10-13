package no.nav.opptjening.api.pgi;

import no.nav.opptjening.dto.InntektDto;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/pensjonsgivendeinntekt")
public class PensjonsgivendeInntekt {

    @RequestMapping(method = RequestMethod.GET, value = "/{inntektsAar}/{personidentifikator}")
    public InntektDto lagTilfeldigInntekt(@PathVariable String inntektsAar, @PathVariable String personidentifikator) {
        return InntektFactory.create(personidentifikator, inntektsAar);
    }
}
