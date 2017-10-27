package no.nav.opptjening.api.hendelser;

import no.nav.opptjening.api.hendelser.repository.HendelseRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class HendelseService {

    private static final Logger LOG = LoggerFactory.getLogger(HendelseService.class);

    private HendelseRepository repository;

    public HendelseService(HendelseRepository repository) {
        this.repository = repository;
    }

    List<HendelseDto> hentHendelser(int sekvesnummer, int antall) {
        LOG.info("Henter {} hendelser fom. sekvensnummer {}", antall, sekvesnummer);

        return repository.findBySekvensnummerGreaterThanEqual(sekvesnummer, new PageRequest(0, antall)).getContent().stream()
                .map(hendelse -> new HendelseDto(
                        hendelse.getSekvensnummer(),
                        hendelse.getIdentifikator(),
                        hendelse.getGjelderPeriode()
                ))
                .collect(Collectors.toList());
    }
}
