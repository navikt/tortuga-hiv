package no.nav.opptjening.skatt.api.hendelser;

import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;
import retrofit2.http.Query;

import java.time.LocalDate;

public interface HendelserApi {

    @GET("/api/{domene}/{ressurs}/hendelser/start")
    Call<SekvensBean> sekvensnummerEtter(@Path("domene") String domene, @Path("ressurs") String ressurs,
                                         @Query("dato") LocalDate dato);

    @GET("/api/{domene}/{ressurs}/hendelser")
    Call<HendelseResponsBean> getHendelser(@Path("domene") String domene, @Path("ressurs") String ressurs,
                                           @Query("fraSekvensnummer") long fraSekvens, @Query("antall") int antall);
}
