package no.nav.opptjening.skatt;

import no.nav.opptjening.skatt.api.FeilmeldingDto;
import no.nav.opptjening.skatt.exceptions.*;

public class ExceptionMapper {
    private ApiException mapFeilmeldingDtoToException(FeilmeldingDto feilmeldingDto, Throwable cause) {
        switch (feilmeldingDto.getKode()) {
            case "FA-001":
                return new MissingSekvensnummerException("fraSekvensnummer må være satt", cause);
            case "FA-002":
                return new IllegalSekvensnummerException("fraSekvensnummer må være større enn 0", cause);
            case "FA-003":
                return new MissingAntallException("antall må være satt", cause);
            case "FA-004":
                return new IllegalAntallException("antall må være større enn 0", cause);
            case "FA-005":
            case "PIA-001":
                // Fant ikke autentiseringstoken
                return null;
            case "FA-006":
            case "PIA-002":
                // Autentisering feilet
                return null;
            case "FA-007":
            case "PIA-004":
                // Den forespurte informasjonen er for øyeblikket utilgjengelig, vennligst prøv igjen senere! Dersom problemet vedvarer, ta kontakt med brukerstøtte!
                return null;
            case "FA-008":
                return new DatabaseUnavailableException("Fikk ikke lagret data i databasen, vennligst prøv igjen senere!", cause);
            case "FA-009":
                return new DatabaseUnavailableException("Fikk ikke hentet data fra databasen, vennligst prøv igjen senere!", cause);
            case "FA-010":
                return new EmptyResultException("Fant ingen informasjon i databasen", cause);
            case "FA-011":
                return new EmptyResultException("Fikk ikke noe resultat for oppgitt person og ressurs.", cause);
            case "FA-012":
                return new EmptyResultException("Den oppgitte ressursen finnes ikke.", cause);
            case "FA-013":
                // Date har ugyldig format. Forventet YYYY-MM-DD.
                return null;
            case "FA-014":
                return new NoSuchSekvensnummerException("Fant ingen sekvensnummer.", cause);
            case "FA-015":
                // Ugyldig dokumenttype.
                return null;

            case "PIA-003":
                // Du er ikke autorisert for bruk av dette endepunktet.
                return null;
            case "PIA-005":
                return new UnsupportedYearException("Det forespurte inntektsåret er ikke støttet", cause);
            case "PIA-006":
                return new MissingInntektException("Fant ikke PGI for gitt inntektsår og identifikator", cause);
        }

        return null;
    }

    public ApiException mapException(FeilmeldingDto feilmeldingDto, Throwable cause) {
        ApiException ex = mapFeilmeldingDtoToException(feilmeldingDto, cause);

        if (ex == null) {
            /*NoSuchElementException e = new NoSuchElementException("Kan ikke mappe " + feilmeldingDto.toString() + " til exception");
            e.initCause(cause);
            throw e;*/
        }

        return ex;
    }
}
