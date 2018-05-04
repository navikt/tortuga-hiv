package no.nav.opptjening.skatt.api.hendelseliste;

import no.nav.opptjening.skatt.api.AbstractClient;
import no.nav.opptjening.skatt.api.FeilmeldingMapper;
import no.nav.opptjening.skatt.schema.hendelsesliste.Hendelsesliste;
import no.nav.opptjening.skatt.schema.hendelsesliste.Sekvensnummer;
import retrofit2.Call;

import java.io.IOException;
import java.time.LocalDate;

public abstract class HendelserClient extends AbstractClient<HendelserApi> {

    public HendelserClient(String endepunkt) {
        super(endepunkt, HendelserApi.class);
    }

    public Sekvensnummer forsteSekvensEtter(LocalDate dato) throws IOException {
        Call<Sekvensnummer> request = getApi().sekvensnummerEtter(dato);
        return executeRequest(request);
    }

    public Hendelsesliste getHendelser(long fraSekvens, int antall) throws IOException {
        Call<Hendelsesliste> request = getApi().getHendelser(fraSekvens, antall);
        return executeRequest(request);
    }

    @Override
    protected FeilmeldingMapper getExceptionMapper() {
        return new HendelselisteExceptionMapper();
    }
}
