package no.nav.opptjening.api.pgi;

import no.nav.opptjening.api.ApiException;
import no.nav.opptjening.api.hendelser.HendelseService;
import no.nav.opptjening.api.pgi.domain.Inntekt;
import no.nav.opptjening.api.pgi.repository.InntektRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.time.Year;

@Service
public class InntektService {

    private static final Logger LOG = LoggerFactory.getLogger(HendelseService.class);

    private InntektRepository repository;

    public InntektService(InntektRepository repository) {
        this.repository = repository;
    }

    InntektDto hentInntekt(String fnr, String inntektsaar) {
        if (Integer.valueOf(inntektsaar) > Year.now().getValue()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "PIA-005", "Det forespurte inntektsåret er ikke støttet");
        }

        LOG.info("Henter inntekt ({},{})", fnr, inntektsaar);

        Inntekt inntekt = repository.findOne(new Inntekt.PersonInntekt(fnr, inntektsaar));

        if (inntekt == null) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "PIA-006", "Fant ikke PGI for gitt inntektsår og identifikator");
        }

        return new InntektDto(inntekt.getIdentifikator(), inntekt.getInntektsaar(), inntekt.getPensjonsgivendeInntekt());
    }
}
