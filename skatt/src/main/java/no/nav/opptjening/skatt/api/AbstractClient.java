package no.nav.opptjening.skatt.api;

import no.nav.opptjening.skatt.api.exceptions.ResponseUnmappableException;
import no.nav.opptjening.skatt.api.exceptions.UkjentFeilkodeException;
import no.nav.opptjening.skatt.exceptions.*;
import no.nav.opptjening.skatt.schema.hendelsesliste.Feilmelding;
import okhttp3.*;
import org.apache.avro.AvroRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;

import java.io.IOException;

public abstract class AbstractClient<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractClient.class);

    private T api;

    private HttpExceptionMapper httpExceptionMapper = new HttpExceptionMapper.BasicHttpExceptionMapper();

    private SkatteetatenErrorResponseMapper errorResponseMapper;

    protected AbstractClient(String endepunkt, Class<T> api) {
        Retrofit retrofit = createRetrofit(endepunkt);

        this.api = retrofit.create(api);
        this.errorResponseMapper = new SkatteetatenErrorResponseMapper(retrofit);
    }

    private static Retrofit createRetrofit(String baseUrl) {
        HttpLoggingInterceptor interceptor = new HttpLoggingInterceptor(new RequestResponseLogger());

        return new Retrofit.Builder()
                .client(new OkHttpClient.Builder().addInterceptor(interceptor).build())
                .baseUrl(baseUrl)
                .addConverterFactory(AvroConverterFactory.create())
                .build();
    }

    protected T getApi() {
        return api;
    }

    abstract protected FeilmeldingMapper getExceptionMapper();

    protected <S> S executeRequest(Call<S> request) throws HttpException, IOException {
        Response<S> response;
        try {
            response = request.execute();
        } catch (AvroRuntimeException e) {
            throw new ResponseUnmappableException("Kan ikke mappe JSON-respons til den angitte klassen på grunn av skjemafeil", e);
        }

        if (response.isSuccessful()) {
            return response.body();
        }

        throw handleErrorResponse(response);
    }

    private <S> HttpException handleErrorResponse(Response<S> response) {
        try {
            Feilmelding error = errorResponseMapper.mapErrorResponseToFeilmelding(response);

            HttpException ex = getExceptionMapper().mapFeilmeldingToHttpException(error, null);

            if (ex != null) {
                return ex;
            }

            return new UkjentFeilkodeException(response.code(),
                    "Kunne ikke mappe Feilmelding=" + error.toString() + " til HttpException pga ukjent feilkode", null);
        } catch (Exception e) {
            return httpExceptionMapper.mapResponseToHttpException(response, e);
        }
    }

}
