package no.nav.opptjening.skatt.api.beregnetskatt;

import no.nav.opptjening.skatt.api.hendelser.HendelserClient;

public class BeregnetSkattHendelserClient extends HendelserClient {

    public BeregnetSkattHendelserClient(String endepunkt) {
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
