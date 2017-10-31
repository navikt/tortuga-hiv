package no.nav.opptjening.skatt.api.pgi;

import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;

public interface InntektApi {

    @GET("/api/formueinntekt/pensjonsgivendeinntekt/{inntektsaar}/{pid}")
    Call<InntektBean> getInntekt(@Path("inntektsaar") String inntektsaar, @Path("pid") String pid);
}
