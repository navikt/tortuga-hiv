package no.nav.opptjening.api.hendelser;

import java.util.List;

import org.springframework.data.repository.Repository;

public interface InntektsfeedRepository extends Repository<InntektHendelse, Long> {

    List<InntektHendelse> findAll();

    List<InntektHendelse> findByFnr(String fnr);

    List<InntektHendelse> findByGjelderPeriodeAfter(String gjelderPeriode);
}