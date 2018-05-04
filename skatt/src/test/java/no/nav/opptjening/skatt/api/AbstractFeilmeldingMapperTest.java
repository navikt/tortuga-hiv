package no.nav.opptjening.skatt.api;

import junit.framework.TestCase;
import no.nav.opptjening.skatt.exceptions.HttpException;
import org.junit.Assert;

import java.util.List;

public abstract class AbstractFeilmeldingMapperTest {

    public void runTests(FeilmeldingMapper exceptionMapper, List<FeilmeldingTestCase> testCases) throws Exception {
        for (FeilmeldingTestCase testCase : testCases) {
            HttpException exception = exceptionMapper.mapFeilmeldingToHttpException(testCase.getFeilmelding(), null);

            if (exception == null) {
                TestCase.fail("Did not expect mapFeilmeldingToHttpException to return null for " + testCase.getFeilmelding());
            }

            Assert.assertEquals(testCase.getExpectedExceptionClass(), exception.getClass());
            Assert.assertEquals(testCase.getExpectedHttpStatus(), exception.getHttpStatus());
            Assert.assertEquals(testCase.getExpectedMessage(), exception.getMessage());
        }
    }
}
