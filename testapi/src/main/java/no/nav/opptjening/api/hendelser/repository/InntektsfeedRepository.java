package no.nav.opptjening.api.hendelser.repository;

import java.util.List;

import no.nav.opptjening.api.hendelser.domain.InntektHendelse;
import org.springframework.data.repository.Repository;

public interface InntektsfeedRepository extends Repository<InntektHendelse, Long> {

    List<InntektHendelse> findAll();

    List<InntektHendelse> findByFnr(String fnr);

    List<InntektHendelse> findByGjelderPeriodeAfter(String gjelderPeriode);
}