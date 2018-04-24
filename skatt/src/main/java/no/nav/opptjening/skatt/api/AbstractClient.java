package no.nav.opptjening.skatt.api;

import no.nav.opptjening.skatt.ExceptionMapper;
import no.nav.opptjening.skatt.exceptions.*;
import no.nav.opptjening.skatt.schema.hendelsesliste.Feilmelding;
import okhttp3.*;
import okio.Buffer;
import okio.BufferedSource;
import org.apache.avro.AvroTypeException;
import org.codehaus.jackson.JsonParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Converter;
import retrofit2.Response;
import retrofit2.Retrofit;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.nio.charset.Charset;

public abstract class AbstractClient<T> {

    private Retrofit retrofit;

    private ExceptionMapper exceptionMapper;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractClient.class);

    private T api;

    protected AbstractClient(String endepunkt, Class<T> api) {
        HttpLoggingInterceptor interceptor = new HttpLoggingInterceptor(new HttpLogger());

        this.retrofit = new Retrofit.Builder()
                .client(new OkHttpClient.Builder().addInterceptor(interceptor).build())
                .baseUrl(endepunkt)
                .addConverterFactory(AvroConverterFactory.create())
                .build();
        this.api = retrofit.create(api);

        this.exceptionMapper = new ExceptionMapper();
    }

    protected T getApi() {
        return api;
    }

    protected <T> T call(Call<T> request) throws ApiException, IOException {

        // throws IOException
        Response<T> response = null;
        try {
            response = request.execute();
        } catch (AvroTypeException e) {
            throw new ResponseMappingException(0, "Kan ikke mappe JSON-respons til den angitte klassen", e);
        }

        if (response.isSuccessful()) {
            return response.body();
        }

        throw handleErrorResponse(response);
    }

    private <T> ApiException handleErrorResponse(Response<T> response) {
        try {
            return mapToApiException(response);
        } catch (JsonParseException e) {
            // not valid JSON, continue
        } catch (AvroTypeException e) {
            return new UnmappableException(response.code(), "Kunne ikke mappe respons til Feilmelding", e);
        } catch (IOException e) {
            return new UnmappableException(response.code(), "Kunne ikke mappe respons til Feilmelding", e);
        }

        if (response.code() < 500) {
            return new ClientException(response.code(), "Ukjent feil fra Skatteetaten", null);
        }

        return new ServerException(response.code(), "Ukjent feil fra Skatteetaten", null);
    }

    private <T> ApiException mapToApiException(Response<T> response) throws IOException {
        Converter<ResponseBody, Feilmelding> errorConverter = retrofit.responseBodyConverter(
                Feilmelding.class, new Annotation[0]);
        Feilmelding error = errorConverter.convert(response.errorBody());

        ApiException ex = exceptionMapper.mapException(error, null);

        if (ex != null) {
            return ex;
        }

        return new UnknownException(response.code(),
                "Kunne ikke mappe Feilmelding=" + error.toString() + " til ApiException pga ukjent feilkode", null);
    }

    private static class HttpLogger implements HttpLoggingInterceptor.Logger {
        @Override
        public void log(Request request) {
            StringBuilder sb = new StringBuilder();
            sb.append(request.method())
                    .append(' ')
                    .append(request.url());

            Headers headers = request.headers();
            int count = headers.size();

            if (count > 0) {
                for (int i = 0; i < count; i++) {
                    String name = headers.name(i);
                    sb.append(name).append(": ").append(headers.value(i)).append('\n');
                }
            }

            LOG.debug(sb.toString());
        }

        @Override
        public void log(okhttp3.Response response) {
            StringBuilder sb = new StringBuilder();

            ResponseBody responseBody = response.body();

            sb.append(response.code()).append(' ').append(response.message()).append('\n');

            Headers headers = response.headers();
            for (int i = 0, count = headers.size(); i < count; i++) {
                sb.append(headers.name(i)).append(": ").append(headers.value(i)).append('\n');
            }

            if (responseBody == null || response.isSuccessful()) {
                LOG.debug(sb.toString());
                return;
            }

            try {
                BufferedSource source = responseBody.source();
                source.request(Long.MAX_VALUE); // Buffer the entire body.
                Buffer buffer = source.buffer();

                Charset charset = Charset.forName("UTF-8");
                MediaType contentType = responseBody.contentType();
                if (contentType != null) {
                    charset = contentType.charset(Charset.forName("UTF-8"));
                }

                sb.append('\n').append(buffer.clone().readString(charset));
            } catch (IOException e) {
                    /* dont log? */
            }

            LOG.debug(sb.toString());
        }
    }
}
