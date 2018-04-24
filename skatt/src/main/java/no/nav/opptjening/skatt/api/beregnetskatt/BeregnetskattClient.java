package no.nav.opptjening.skatt.api.beregnetskatt;

import no.nav.opptjening.skatt.api.AbstractClient;
import no.nav.opptjening.skatt.schema.BeregnetSkatt;
import retrofit2.Call;

import java.io.IOException;

public class BeregnetskattClient extends AbstractClient<BeregnetskattApi> {

    public BeregnetskattClient(String endepunkt) {
        super(endepunkt, BeregnetskattApi.class);
    }

    public BeregnetSkatt getBeregnetSkatt(String rettighetspakke, String inntektsaar, String personidentifikator) throws IOException {
        Call<BeregnetSkatt> request = getApi().getBeregnetSkatt(rettighetspakke, inntektsaar, personidentifikator);
        return call(request);
    }
}
