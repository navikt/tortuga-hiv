package no.nav.opptjening.skatt.api.beregnetskatt;

import no.nav.opptjening.skatt.schema.BeregnetSkatt;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;

public interface BeregnetSkattApi {

    @GET("{rettighetspakke}/{inntektsaar}/{pid}")
    Call<BeregnetSkatt> getBeregnetSkatt(@Path("rettighetspakke") String rettighetspakke, @Path("inntektsaar") String inntektsaar, @Path("pid") String pid);
}
