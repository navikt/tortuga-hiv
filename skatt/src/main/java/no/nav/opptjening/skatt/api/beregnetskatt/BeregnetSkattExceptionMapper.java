package no.nav.opptjening.skatt.api.beregnetskatt;

import no.nav.opptjening.skatt.api.FeilmeldingMapper;
import no.nav.opptjening.skatt.exceptions.HttpException;
import no.nav.opptjening.skatt.exceptions.BadRequestException;
import no.nav.opptjening.skatt.exceptions.NotFoundException;
import no.nav.opptjening.skatt.schema.hendelsesliste.Feilmelding;

public class BeregnetSkattExceptionMapper implements FeilmeldingMapper {
    public HttpException mapFeilmeldingToHttpException(Feilmelding feilmelding, Throwable cause) {
        switch (feilmelding.getKode()) {
            case "BSA-005":
                return new BadRequestException("Det forespurte inntektsåret er ikke støttet", cause);
            case "BSA-006":
                return new BadRequestException("Fant ikke Beregnet Skatt for gitt inntektsår og identifikator", cause);
            case "BSA-007":
                return new BadRequestException("Inntektsår har ikke gyldig format", cause);
            case "BSA-008":
                return new BadRequestException("Personidentifikator har ikke gyldig format", cause);
            case "BSA-009":
                return new NotFoundException("Fant ingen person for gitt identifikator", cause);
        }

        return null;
    }
}
