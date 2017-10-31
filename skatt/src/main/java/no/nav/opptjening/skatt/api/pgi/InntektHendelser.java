package no.nav.opptjening.skatt.api.pgi;

import no.nav.opptjening.skatt.api.SkatteetatenClient;
import no.nav.opptjening.skatt.api.hendelser.Hendelser;
import no.nav.opptjening.skatt.api.hendelser.HendelserApi;

public class InntektHendelser extends Hendelser {

    public InntektHendelser(SkatteetatenClient client, HendelserApi hendelserApi) {
        super(client, hendelserApi);
    }

    @Override
    protected String getDomene() {
        return "formueinntekt";
    }

    @Override
    protected String getRessurs() {
        return "beregnetskatt";
    }
}
