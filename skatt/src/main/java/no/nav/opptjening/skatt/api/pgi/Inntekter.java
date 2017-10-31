package no.nav.opptjening.skatt.api.pgi;

import no.nav.opptjening.skatt.api.SkatteetatenClient;
import no.nav.opptjening.skatt.dto.InntektDto;
import retrofit2.Call;

public class Inntekter {

    private SkatteetatenClient skatteetatenClient;

    private InntektApi inntektApi;

    public Inntekter(SkatteetatenClient skatteetatenClient, InntektApi inntektApi) {
        this.skatteetatenClient = skatteetatenClient;
        this.inntektApi = inntektApi;
    }

    public InntektDto hentInntekt(String inntektsaar, String personidentifikator) {
        Call<InntektBean> request = inntektApi.getInntekt(inntektsaar, personidentifikator);

        InntektBean inntektBean = skatteetatenClient.call(request);
        return new InntektDto(inntektBean.getPersonindentifikator(), inntektBean.getInntektsaar(), inntektBean.getPensjonsgivendeInntekt());
    }
}
