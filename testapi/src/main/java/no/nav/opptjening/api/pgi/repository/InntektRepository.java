package no.nav.opptjening.api.pgi.repository;

import no.nav.opptjening.api.pgi.domain.Inntekt;
import org.springframework.data.repository.Repository;

public interface InntektRepository extends Repository<Inntekt, Inntekt.PersonInntekt> {

    Inntekt findOne(Inntekt.PersonInntekt pk);

}