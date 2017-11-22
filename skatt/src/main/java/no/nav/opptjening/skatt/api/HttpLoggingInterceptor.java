package no.nav.opptjening.skatt.api;

import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;

class HttpLoggingInterceptor implements Interceptor {
    public interface Logger {
        void log(Request request);
        void log(Response request);
    }

    HttpLoggingInterceptor(Logger logger) {
        this.logger = logger;
    }

    private final Logger logger;

    @Override public Response intercept(Chain chain) throws IOException {
        Request request = chain.request();

        logger.log(request);
        Response response = chain.proceed(request);
        logger.log(response);

        return response;
    }
}

