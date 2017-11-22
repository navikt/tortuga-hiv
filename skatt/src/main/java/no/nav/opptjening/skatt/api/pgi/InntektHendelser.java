package no.nav.opptjening.skatt.api.pgi;

import no.nav.opptjening.skatt.api.hendelser.Hendelser;

public class InntektHendelser extends Hendelser {

    public InntektHendelser(String endepunkt) {
        super(endepunkt);
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
