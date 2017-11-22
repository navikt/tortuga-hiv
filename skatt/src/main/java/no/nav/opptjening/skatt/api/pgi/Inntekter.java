package no.nav.opptjening.skatt.api.pgi;

import no.nav.opptjening.skatt.api.AbstractClient;
import retrofit2.Call;

public class Inntekter extends AbstractClient<InntektApi> {

    public Inntekter(String endepunkt) {
        super(endepunkt, InntektApi.class);
    }

    public InntektDto hentInntekt(String inntektsaar, String personidentifikator) {
        Call<InntektBean> request = getApi().getInntekt(inntektsaar, personidentifikator);

        InntektBean inntektBean = call(request);
        return new InntektDto(inntektBean.getPersonindentifikator(), inntektBean.getInntektsaar(), inntektBean.getPensjonsgivendeInntekt());
    }
}
