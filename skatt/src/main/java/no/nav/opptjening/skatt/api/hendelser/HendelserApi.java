package no.nav.opptjening.skatt.api.hendelser;

import no.nav.opptjening.schema.skatteetaten.hendelsesliste.Hendelsesliste;
import no.nav.opptjening.schema.skatteetaten.hendelsesliste.Sekvensnummer;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;
import retrofit2.http.Query;

import java.time.LocalDate;

public interface HendelserApi {

    @GET("{domene}/{ressurs}/hendelser/start")
    Call<Sekvensnummer> sekvensnummerEtter(@Path("domene") String domene, @Path("ressurs") String ressurs,
                                           @Query("dato") LocalDate dato);

    @GET("{domene}/{ressurs}/hendelser")
    Call<Hendelsesliste> getHendelser(@Path("domene") String domene, @Path("ressurs") String ressurs,
                                      @Query("fraSekvensnummer") long fraSekvens, @Query("antall") int antall);
}
