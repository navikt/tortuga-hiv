package no.nav.opptjening.api.pgi;

import no.nav.opptjening.api.hendelser.HendelseService;
import no.nav.opptjening.api.pgi.domain.Inntekt;
import no.nav.opptjening.api.pgi.repository.InntektRepository;
import no.nav.opptjening.skatt.dto.InntektDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class InntektService {

    private static final Logger LOG = LoggerFactory.getLogger(HendelseService.class);

    private InntektRepository repository;

    public InntektService(InntektRepository repository) {
        this.repository = repository;
    }

    InntektDto hentInntekt(String fnr, String inntektsaar) {
        LOG.info("Henter inntekt ({},{})", fnr, inntektsaar);

        Inntekt inntekt = repository.findOne(new Inntekt.PersonInntekt(fnr, inntektsaar));
        return new InntektDto(inntekt.getIdentifikator(), inntekt.getInntektsaar(), inntekt.getPensjonsgivendeInntekt());
    }
}
