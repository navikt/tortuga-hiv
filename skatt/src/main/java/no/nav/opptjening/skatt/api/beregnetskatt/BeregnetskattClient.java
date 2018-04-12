package no.nav.opptjening.skatt.api.beregnetskatt;

import no.nav.opptjening.schema.skatteetaten.BeregnetSkatt;
import no.nav.opptjening.skatt.api.AbstractClient;
import retrofit2.Call;

public class BeregnetskattClient extends AbstractClient<BeregnetskattApi> {

    public BeregnetskattClient(String endepunkt) {
        super(endepunkt, BeregnetskattApi.class);
    }

    public BeregnetSkatt getBeregnetSkatt(String rettighetspakke, String inntektsaar, String personidentifikator) {
        Call<BeregnetSkatt> request = getApi().getBeregnetSkatt(rettighetspakke, inntektsaar, personidentifikator);
        return call(request);
    }
}
