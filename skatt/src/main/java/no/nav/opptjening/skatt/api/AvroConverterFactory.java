package no.nav.opptjening.skatt.api;

import okhttp3.ResponseBody;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import retrofit2.Converter;
import retrofit2.Retrofit;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

public class AvroConverterFactory extends Converter.Factory {
    public AvroConverterFactory() {
        super();
    }

    @Override
    public Converter<ResponseBody, ? extends SpecificRecord> responseBodyConverter(Type type, Annotation[] annotations, Retrofit retrofit) {
        Schema schema = getSchema(getRawType(type));

        if (schema == null) {
            throw new IllegalArgumentException("Unable to get schema for " + type.getTypeName());
        }

        return new AvroResponseBodyConverter<>(schema, type, annotations, retrofit);
    }

    public static Converter.Factory create() {
        return new AvroConverterFactory();
    }

    private Schema getSchema(Class<?> cls) {
        if (!SpecificRecordBase.class.isAssignableFrom(cls)) {
            return null;
        }

        Method method = null;
        Field field = null;

        try {
            method = cls.getMethod("getClassSchema");
        } catch (NoSuchMethodException e) {
            /* continue */
        }

        if (method == null) {
            try {
                field = cls.getDeclaredField("SCHEMA$");
            } catch (NoSuchFieldException e) {
                return null;
            }
        }

        Object schema;
        try {
            if (method == null) {
                schema = field.get(null);
            } else {
                schema = method.invoke(null);
            }
        } catch (IllegalAccessException | InvocationTargetException e) {
            return null;
        }

        if (schema instanceof Schema) {
            return (Schema) schema;
        }

        return null;
    }
}
