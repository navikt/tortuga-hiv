package no.nav.opptjening.hiv;

import no.nav.opptjening.schema.skatt.hendelsesliste.Hendelse;
import no.nav.opptjening.skatt.client.Hendelsesliste;

class HendelseMapper {

    Hendelse mapToHendelse(Hendelsesliste.Hendelse hendelse) {
        return new Hendelse(hendelse.getSekvensnummer(), hendelse.getIdentifikator(), hendelse.getGjelderPeriode());
    }
}
