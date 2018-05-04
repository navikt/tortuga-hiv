package no.nav.opptjening.skatt.api;

import no.nav.opptjening.skatt.exceptions.ClientException;
import no.nav.opptjening.skatt.exceptions.ServerException;
import okhttp3.ResponseBody;
import org.junit.Assert;
import org.junit.Test;
import retrofit2.Response;

public class BasicHttpExceptionMapperTest {

    private final HttpExceptionMapper exceptionMapper = new HttpExceptionMapper.BasicHttpExceptionMapper();

    @Test
    public void map4xxResponseToClientException() {
        Assert.assertEquals(ClientException.class, exceptionMapper.mapResponseToHttpException(
                Response.error(400, ResponseBody.create(null, new byte[0])), null).getClass());
        Assert.assertEquals(ClientException.class, exceptionMapper.mapResponseToHttpException(
                Response.error(499, ResponseBody.create(null, new byte[0])), null).getClass());
    }

    @Test
    public void map5xxResponseToClientException() {
        Assert.assertEquals(ServerException.class, exceptionMapper.mapResponseToHttpException(
                Response.error(500, ResponseBody.create(null, new byte[0])), null).getClass());
        Assert.assertEquals(ServerException.class, exceptionMapper.mapResponseToHttpException(
                Response.error(599, ResponseBody.create(null, new byte[0])), null).getClass());
    }
}
