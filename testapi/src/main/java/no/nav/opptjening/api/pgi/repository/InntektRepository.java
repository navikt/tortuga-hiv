package no.nav.opptjening.api.pgi.repository;

import no.nav.opptjening.api.pgi.domain.Inntekt;
import org.springframework.data.repository.CrudRepository;

public interface InntektRepository extends CrudRepository<Inntekt, Inntekt.PersonInntekt> {

    Inntekt findOne(Inntekt.PersonInntekt pk);

}