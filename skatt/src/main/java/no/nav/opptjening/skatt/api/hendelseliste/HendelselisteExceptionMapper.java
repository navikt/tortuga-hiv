package no.nav.opptjening.skatt.api.hendelseliste;

import no.nav.opptjening.skatt.api.FeilmeldingMapper;
import no.nav.opptjening.skatt.api.hendelseliste.exceptions.*;
import no.nav.opptjening.skatt.exceptions.HttpException;
import no.nav.opptjening.skatt.exceptions.BadRequestException;
import no.nav.opptjening.skatt.schema.hendelsesliste.Feilmelding;

public class HendelselisteExceptionMapper implements FeilmeldingMapper {
    public HttpException mapFeilmeldingToHttpException(Feilmelding feilmelding, Throwable cause) {
        switch (feilmelding.getKode()) {
            case "FA-001":
                return new MissingSekvensnummerException("fraSekvensnummer må være satt", cause);
            case "FA-002":
                return new IllegalSekvensnummerException("fraSekvensnummer må være større enn 0", cause);
            case "FA-003":
                return new MissingAntallException("antall må være spesifisert", cause);
            case "FA-004":
                return new IllegalAntallException("antall må være større enn 0", cause);
            case "FA-009":
                return new DatabaseUnavailableException("Fikk ikke hentet data fra databasen, vennligst prøv igjen senere!", cause);
            case "FA-010":
                return new EmptyResultException("Fant ingen informasjon i databasen", cause);
            case "FA-013":
                return new BadRequestException("Dato har ugyldig format. Forventet YYYY-MM-DD.", cause);
            case "FA-014":
                return new NoSuchSekvensnummerException("Fant ingen sekvensnummer.", cause);
            case "FA-015":
                return new BadRequestException("Ugyldig dokumenttype.", cause);
        }

        return null;
    }
}
