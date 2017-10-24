package no.nav.opptjening.skatt;

import no.nav.opptjening.skatt.dto.FeilmeldingDto;
import no.nav.opptjening.skatt.exceptions.*;
import org.junit.Before;
import org.junit.Test;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map;

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
            FeilmeldingDto feilmeldingDto = new FeilmeldingDto();
            feilmeldingDto.setKode(entry.getKey());

            Class<? extends ApiException> expected = entry.getValue();
            ApiException result = exceptionMapper.mapException(feilmeldingDto, null);

            if (expected == null) {
                Assert.isNull(result, "Expected " + feilmeldingDto.getKode() + " to be null");
            } else {
                Assert.isInstanceOf(expected, result, "Expected " + feilmeldingDto.getKode() + " to be instance of " + expected.getSimpleName());
            }
        }
    }

}