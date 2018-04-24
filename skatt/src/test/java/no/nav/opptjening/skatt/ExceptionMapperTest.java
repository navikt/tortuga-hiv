package no.nav.opptjening.skatt;

import no.nav.opptjening.skatt.exceptions.*;
import no.nav.opptjening.skatt.schema.hendelsesliste.Feilmelding;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;

public class ExceptionMapperTest {

    Map<String, Class<? extends ApiException>> testCases;

    @Before
    public void setUp() {
        testCases = new HashMap<>();

        testCases.put("FA-001", MissingSekvensnummerException.class);
        testCases.put("FA-002", IllegalSekvensnummerException.class);
        testCases.put("FA-003", MissingAntallException.class);
        testCases.put("FA-004", IllegalAntallException.class);
        testCases.put("FA-005", null);
        testCases.put("FA-006", null);
        testCases.put("FA-007", null);
        testCases.put("FA-008", DatabaseUnavailableException.class);
        testCases.put("FA-009", DatabaseUnavailableException.class);
        testCases.put("FA-010", EmptyResultException.class);
        testCases.put("FA-011", EmptyResultException.class);
        testCases.put("FA-012", EmptyResultException.class);
        testCases.put("FA-013", null);
        testCases.put("FA-014", NoSuchSekvensnummerException.class);
        testCases.put("FA-015", null);
        testCases.put("PIA-001", null);
        testCases.put("PIA-002", null);
        testCases.put("PIA-003", null);
        testCases.put("PIA-004", null);
        testCases.put("PIA-005", UnsupportedYearException.class);
        testCases.put("PIA-006", MissingInntektException.class);
    }

    @Test
    public void mapException() throws Exception {
        ExceptionMapper exceptionMapper = new ExceptionMapper();

        for (Map.Entry<String, Class<? extends ApiException>> entry : testCases.entrySet()) {
            Feilmelding feilmelding = Feilmelding.newBuilder()
                    .setKode(entry.getKey())
                    .setMelding("")
                    .build();

            Class<? extends ApiException> expected = entry.getValue();
            ApiException result = exceptionMapper.mapException(feilmelding, null);

            if (expected == null) {
                Assert.assertNull("Expected " + feilmelding.getKode() + " to be null", result);
            } else {
                Assert.assertThat(result, instanceOf(expected));
            }
        }
    }

}
