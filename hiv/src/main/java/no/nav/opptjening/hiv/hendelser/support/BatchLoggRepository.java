package no.nav.opptjening.hiv.hendelser.support;


import org.springframework.data.repository.CrudRepository;

public interface BatchLoggRepository extends CrudRepository<BatchLoggEntry, Long> {

    BatchLoggEntry findTop1ByOrderByJobIdDesc();
}

