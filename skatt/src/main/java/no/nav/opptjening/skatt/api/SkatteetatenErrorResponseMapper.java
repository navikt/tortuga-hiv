package no.nav.opptjening.skatt.api;

import no.nav.opptjening.skatt.api.exceptions.ResponseUnmappableException;
import no.nav.opptjening.skatt.schema.hendelsesliste.Feilmelding;
import okhttp3.ResponseBody;
import org.apache.avro.AvroRuntimeException;
import retrofit2.Converter;
import retrofit2.Response;
import retrofit2.Retrofit;

import java.io.IOException;
import java.lang.annotation.Annotation;

class SkatteetatenErrorResponseMapper {
    private final Retrofit retrofit;

    public SkatteetatenErrorResponseMapper(Retrofit retrofit) {
        this.retrofit = retrofit;
    }

    public <S> Feilmelding mapErrorResponseToFeilmelding(Response<S> response) {
        Converter<ResponseBody, Feilmelding> errorConverter = retrofit.responseBodyConverter(
                Feilmelding.class, new Annotation[0]);

        try {
            return errorConverter.convert(response.errorBody());
        } catch (AvroRuntimeException e) {
            throw new ResponseUnmappableException("Kan ikke mappe respons til Feilmelding på grunn av skjemafeil", e);
        } catch (IOException e) {
            throw new ResponseUnmappableException("Kan ikke mappe respons til Feilmelding", e);
        }
    }
}
