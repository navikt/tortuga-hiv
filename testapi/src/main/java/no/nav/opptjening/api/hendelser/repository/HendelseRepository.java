package no.nav.opptjening.api.hendelser.repository;

import no.nav.opptjening.api.hendelser.domain.Hendelse;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.Repository;

public interface HendelseRepository extends Repository<Hendelse, Integer> {

    Iterable<Hendelse> findAll();

    Page<Hendelse> findAll(Pageable pageable);

    Iterable<Hendelse> findByIdentifikator(String fnr);

    Page<Hendelse> findBySekvensnummerGreaterThanEqual(int sekvensnummer, Pageable pageable);
}