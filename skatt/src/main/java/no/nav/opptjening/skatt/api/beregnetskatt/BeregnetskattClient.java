package no.nav.opptjening.skatt.api.beregnetskatt;

import no.nav.opptjening.skatt.api.AbstractClient;
import no.nav.opptjening.skatt.api.FeilmeldingMapper;
import no.nav.opptjening.skatt.schema.BeregnetSkatt;
import retrofit2.Call;

import java.io.IOException;

public class BeregnetSkattClient extends AbstractClient<BeregnetSkattApi> {

    public BeregnetSkattClient(String endepunkt) {
        super(endepunkt, BeregnetSkattApi.class);
    }

    public BeregnetSkatt getBeregnetSkatt(String rettighetspakke, String inntektsaar, String personidentifikator) throws IOException {
        Call<BeregnetSkatt> request = getApi().getBeregnetSkatt(rettighetspakke, inntektsaar, personidentifikator);
        return executeRequest(request);
    }

    @Override
    protected FeilmeldingMapper getExceptionMapper() {
        return new BeregnetSkattExceptionMapper();
    }
}
