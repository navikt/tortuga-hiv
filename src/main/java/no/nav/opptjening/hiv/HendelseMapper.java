package no.nav.opptjening.hiv;

import no.nav.opptjening.schema.skatt.hendelsesliste.Hendelse;
import no.nav.opptjening.skatt.client.Hendelsesliste;
import org.jetbrains.annotations.NotNull;

public class HendelseMapper {

    @NotNull
    public Hendelse mapToHendelse(@NotNull Hendelsesliste.Hendelse hendelse) {
        return new Hendelse(hendelse.getSekvensnummer(), hendelse.getIdentifikator(), hendelse.getGjelderPeriode());
    }
}
