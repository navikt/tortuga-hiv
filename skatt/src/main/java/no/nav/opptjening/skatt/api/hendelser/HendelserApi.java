package no.nav.opptjening.skatt.api.hendelser;

import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Query;

import java.time.LocalDate;

public interface HendelserApi {

    @GET("/start")
    Call<SekvensBean> sekvensnummerEtter(@Query("dato") LocalDate dato);

    @GET("/")
    Call<HendelseResponsBean> getHendelser(@Query("fraSekvensnummer") long fraSekvens, @Query("antall") int antall);
}
