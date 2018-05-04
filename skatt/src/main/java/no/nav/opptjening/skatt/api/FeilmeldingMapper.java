package no.nav.opptjening.skatt.api;

import no.nav.opptjening.skatt.exceptions.HttpException;
import no.nav.opptjening.skatt.schema.hendelsesliste.Feilmelding;

public interface FeilmeldingMapper {
    HttpException mapFeilmeldingToHttpException(Feilmelding feilmelding, Throwable cause);
}
