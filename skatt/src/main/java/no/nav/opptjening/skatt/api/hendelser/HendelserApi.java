package no.nav.opptjening.skatt.api.hendelser;

import no.nav.opptjening.skatt.schema.hendelsesliste.Hendelsesliste;
import no.nav.opptjening.skatt.schema.hendelsesliste.Sekvensnummer;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Query;

import java.time.LocalDate;

public interface HendelserApi {

    @GET("hendelser/start")
    Call<Sekvensnummer> sekvensnummerEtter(@Query("dato") LocalDate dato);

    @GET("hendelser")
    Call<Hendelsesliste> getHendelser(@Query("fraSekvensnummer") long fraSekvens, @Query("antall") int antall);
}
