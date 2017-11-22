package no.nav.opptjening.skatt.api.hendelser;

import no.nav.opptjening.skatt.api.AbstractClient;
import retrofit2.Call;

import java.time.LocalDate;
import java.util.List;

public abstract class Hendelser extends AbstractClient<HendelserApi> {

    public Hendelser(String endepunkt) {
        super(endepunkt, HendelserApi.class);
    }

    protected abstract String getDomene();
    protected abstract String getRessurs();

    public SekvensDto forsteSekvensEtter(LocalDate dato) {
        Call<SekvensBean> request = getApi().sekvensnummerEtter(getDomene(), getRessurs(), dato);
        return new SekvensDto(call(request).getSekvensnummer());
    }

    public List<HendelseDto> getHendelser(long fraSekvens, int antall) {
        Call<HendelseResponsBean> request = getApi().getHendelser(getDomene(), getRessurs(), fraSekvens, antall);
        return call(request).getHendelser();
    }
}
