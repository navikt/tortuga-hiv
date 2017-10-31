package no.nav.opptjening.skatt.api;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import no.nav.opptjening.skatt.ExceptionMapper;
import no.nav.opptjening.skatt.api.hendelser.Hendelser;
import no.nav.opptjening.skatt.api.hendelser.HendelserApi;
import no.nav.opptjening.skatt.api.pgi.InntektApi;
import no.nav.opptjening.skatt.api.pgi.InntektHendelser;
import no.nav.opptjening.skatt.api.pgi.Inntekter;
import no.nav.opptjening.skatt.dto.FeilmeldingDto;
import no.nav.opptjening.skatt.exceptions.*;
import okhttp3.OkHttpClient;
import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.Converter;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.lang.annotation.Annotation;

public class SkatteetatenClient {

    private Retrofit retrofit;

    private ExceptionMapper exceptionMapper;

    public SkatteetatenClient(String endepunkt) {
        this.retrofit = new Retrofit.Builder()
                .client(new OkHttpClient.Builder().build())
                .baseUrl(endepunkt)
                .addConverterFactory(JacksonConverterFactory.create())
                .build();

        this.exceptionMapper = new ExceptionMapper();
    }

    public Hendelser getInntektHendelser() {
        return new InntektHendelser(this, getHendelseApi());
    }

    private HendelserApi getHendelseApi() {
        return retrofit.create(HendelserApi.class);
    }

    public Inntekter getInntekter() {
        return new Inntekter(this, getInntektApi());
    }

    private InntektApi getInntektApi() {
        return retrofit.create(InntektApi.class);
    }

    public <T> T call(Call<T> request) throws ApiException {
        // throws IOException
        Response<T> response = null;
        try {
            response = request.execute();
        } catch (JsonMappingException e) {
            throw new ResponseMappingException(0, "Kan ikke mappe JSON-respons til den angitte klassen", e);
        } catch (IOException e) {
            // network error?
            throw new RuntimeException(e);
        }

        if (response.isSuccessful()) {
            return response.body();
        }

        Converter<ResponseBody, FeilmeldingDto> errorConverter = retrofit.responseBodyConverter(
                FeilmeldingDto.class, new Annotation[0]);

        try {
            FeilmeldingDto error = errorConverter.convert(response.errorBody());

            ApiException ex = exceptionMapper.mapException(error, null);

            if (ex == null) {
                throw new UnknownException(response.code(),
                        "Kunne ikke mappe FeilmeldingDto=" + error.toString() + " til ApiException pga ukjent feilkode", null);
            }

            throw ex;
        } catch (JsonParseException e) {
            // not valid JSON, continue
        } catch (IOException e) {
            throw new UnmappableException(response.code(), "Kunne ikke mappe respons til FeilmeldingDto", e);
        }

        if (response.code() < 500) {
            throw new ClientException(response.code(), "Ukjent feil fra Skatteetaten", null);
        } else {
            throw new ServerException(response.code(), "Ukjent feil fra Skatteetaten", null);
        }
    }
}
