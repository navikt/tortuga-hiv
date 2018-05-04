package no.nav.opptjening.skatt.api;

import okhttp3.*;
import okio.Buffer;
import okio.BufferedSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;

class RequestResponseLogger implements HttpLoggingInterceptor.Logger {
    private static final Logger LOG = LoggerFactory.getLogger(RequestResponseLogger.class);

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
    public void log(Response response) {
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
                charset = contentType.charset(charset);
            }

            sb.append('\n').append(buffer.clone().readString(charset));
        } catch (IOException e) {
            /* dont log? */
        }

        LOG.debug(sb.toString());
    }
}
