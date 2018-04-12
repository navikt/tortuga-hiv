package no.nav.opptjening.skatt.api;

import okhttp3.ResponseBody;
import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import retrofit2.Converter;
import retrofit2.Retrofit;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

public class AvroResponseBodyConverter<T extends SpecificRecord> implements Converter<ResponseBody, T> {

    private final Schema schema;
    private final Type type;
    private final Annotation[] annotations;
    private final Retrofit retrofit;

    public AvroResponseBodyConverter(Schema schema, Type type, Annotation[] annotations, Retrofit retrofit) {
        if (schema == null) {
            throw new IllegalArgumentException("schema cannot be null");
        }

        this.schema = schema;
        this.type = type;
        this.annotations = annotations;
        this.retrofit = retrofit;
    }

    @Override
    public T convert(ResponseBody value) throws IOException {
        SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema);
        JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, value.byteStream());

        return reader.read(null, jsonDecoder);
    }
}
