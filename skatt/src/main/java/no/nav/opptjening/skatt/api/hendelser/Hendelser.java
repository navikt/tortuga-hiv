package no.nav.opptjening.skatt.api.hendelser;

import no.nav.opptjening.skatt.api.SkatteetatenClient;
import no.nav.opptjening.skatt.dto.HendelseDto;
import no.nav.opptjening.skatt.dto.SekvensDto;
import retrofit2.Call;

import java.time.LocalDate;
import java.util.List;

public class Hendelser {

    private SkatteetatenClient skatteetatenClient;

    private HendelserApi hendelserApi;

    public Hendelser(String endepunkt){
        skatteetatenClient = new SkatteetatenClient(endepunkt);
        hendelserApi = skatteetatenClient.getHendelseApi();
    }

    public SekvensDto forsteSekvensEtter(LocalDate dato) {
        Call<SekvensBean> request = hendelserApi.sekvensnummerEtter(dato);

        SekvensBean sekvens = skatteetatenClient.call(request);
        return new SekvensDto(sekvens.getSekvensnummer());
    }

    public List<HendelseDto> getHendelser(long fraSekvens, int antall) {
        Call<HendelseResponsBean> request = hendelserApi.getHendelser(fraSekvens, antall);

        HendelseResponsBean hendelseRespons = skatteetatenClient.call(request);
        return hendelseRespons.getHendelser();
    }
}
