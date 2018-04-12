package no.nav.opptjening.skatt.api.hendelser;

import no.nav.opptjening.schema.skatteetaten.hendelsesliste.Hendelsesliste;
import no.nav.opptjening.schema.skatteetaten.hendelsesliste.Sekvensnummer;
import no.nav.opptjening.skatt.api.AbstractClient;
import retrofit2.Call;

import java.time.LocalDate;

public abstract class HendelserClient extends AbstractClient<HendelserApi> {

    public HendelserClient(String endepunkt) {
        super(endepunkt, HendelserApi.class);
    }

    protected abstract String getDomene();
    protected abstract String getRessurs();

    public Sekvensnummer forsteSekvensEtter(LocalDate dato) {
        Call<Sekvensnummer> request = getApi().sekvensnummerEtter(getDomene(), getRessurs(), dato);
        return call(request);
    }

    public Hendelsesliste getHendelser(long fraSekvens, int antall) {
        Call<Hendelsesliste> request = getApi().getHendelser(getDomene(), getRessurs(), fraSekvens, antall);
        return call(request);
    }
}
