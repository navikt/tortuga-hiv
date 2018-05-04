package no.nav.opptjening.skatt.api.hendelseliste;

import no.nav.opptjening.skatt.api.AbstractFeilmeldingMapperTest;
import no.nav.opptjening.skatt.api.FeilmeldingTestCase;
import no.nav.opptjening.skatt.api.hendelseliste.exceptions.*;
import no.nav.opptjening.skatt.exceptions.BadRequestException;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HendelselisteExceptionMapperTest extends AbstractFeilmeldingMapperTest {
    private static final List<FeilmeldingTestCase> testCases = Arrays.asList(new FeilmeldingTestCase("FA-001", "fraSekvensnummer må være satt",
                    400, MissingSekvensnummerException.class, "fraSekvensnummer må være satt"),
            new FeilmeldingTestCase("FA-002", "fraSekvensnummer må være større enn 0",
                    400, IllegalSekvensnummerException.class, "fraSekvensnummer må være større enn 0"),
            new FeilmeldingTestCase("FA-003", "antall må være spesifisert",
                    400, MissingAntallException.class, "antall må være spesifisert"),
            new FeilmeldingTestCase("FA-004", "antall må være større enn 0",
                    400, IllegalAntallException.class, "antall må være større enn 0"),
            new FeilmeldingTestCase("FA-009", "Fikk ikke hentet data fra databasen, vennligst prøv igjen senere!",
                    500, DatabaseUnavailableException.class, "Fikk ikke hentet data fra databasen, vennligst prøv igjen senere!"),
            new FeilmeldingTestCase("FA-010", "Fant ingen informasjon i databasen",
                    404, EmptyResultException.class, "Fant ingen informasjon i databasen"),
            new FeilmeldingTestCase("FA-013", "Dato har ugyldig format. Forventet YYYY-MM-DD.",
                    400, BadRequestException.class, "Dato har ugyldig format. Forventet YYYY-MM-DD."),
            new FeilmeldingTestCase("FA-014", "Fant ingen sekvensnummer.",
                    404, NoSuchSekvensnummerException.class, "Fant ingen sekvensnummer."),
            new FeilmeldingTestCase("FA-015", "Ugyldig dokumenttype.",
                    400, BadRequestException.class, "Ugyldig dokumenttype."));

    @Test
    public void mapFeilmeldingToHttpException() throws Exception {
        runTests(new HendelselisteExceptionMapper(), testCases);
    }

}
