package no.nav.opptjening.skatt.api;

import no.nav.opptjening.skatt.api.exceptions.ResponseUnmappableException;
import no.nav.opptjening.skatt.schema.hendelsesliste.Feilmelding;
import okhttp3.MediaType;
import okhttp3.ResponseBody;
import org.junit.Assert;
import org.junit.Test;
import retrofit2.Response;
import retrofit2.Retrofit;

public class SkatteetatenErrorResponseMapperTest {
    private final Retrofit retrofit = new Retrofit.Builder()
            .addConverterFactory(AvroConverterFactory.create())
            .baseUrl("http://test")
            .build();

    private final SkatteetatenErrorResponseMapper responseMapper = new SkatteetatenErrorResponseMapper(retrofit);

    @Test(expected = ResponseUnmappableException.class)
    public void when_ResponseDoesNotMatchSchema_Then_ThrowUnmappableException() {
        Response<Feilmelding> response = Response.error(404, ResponseBody.create(MediaType.parse("application/json"),
                "{\"feilkode\": \"ZZ-001\", \"feilmelding\": \"Denne feilkoden er ikke implementert\"}"));

        responseMapper.mapErrorResponseToFeilmelding(response);
    }

    @Test
    public void when_ResponseContainsFeilmelding_Then_ValuesAreMappedCorrectly() {
        Response<Feilmelding> response = Response.error(404, ResponseBody.create(MediaType.parse("application/json"),
                "{\"kode\": \"ZZ-001\", \"melding\": \"Denne feilkoden er ikke implementert\"}"));

        Feilmelding result = responseMapper.mapErrorResponseToFeilmelding(response);


        Assert.assertEquals("ZZ-001", result.getKode());
        Assert.assertEquals("Denne feilkoden er ikke implementert", result.getMelding());
    }
}
