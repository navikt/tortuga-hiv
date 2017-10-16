package no.nav.opptjening.api.hendelser;

import no.nav.opptjening.api.hendelser.domain.Hendelse;
import no.nav.opptjening.api.hendelser.repository.HendelseRepository;
import no.nav.opptjening.skatt.dto.HendelseDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class HendelseService {

    private static final Logger LOG = LoggerFactory.getLogger(HendelseService.class);

    private HendelseRepository repository;

    public HendelseService(HendelseRepository repository) {
        this.repository = repository;
    }

    List<HendelseDto> hentHendelser() {
        LOG.info("Henter alle hendelser");
        List<HendelseDto> hendelser = new ArrayList<>();
        for (Hendelse hendelse : repository.findAll()) {
            hendelser.add(new HendelseDto(
                    hendelse.getSekvensnummer(),
                    hendelse.getIdentifikator(),
                    hendelse.getGjelderPeriode()
            ));
        }
        return hendelser;
    }

    List<HendelseDto> hentHendelser(int sekvesnummer, int antall) {
        LOG.info("Henter hendelser etter {}", sekvesnummer);

        return repository.findBySekvensnummerAfter(sekvesnummer, new PageRequest(0, antall)).getContent().stream()
                .map(hendelse -> new HendelseDto(
                        hendelse.getSekvensnummer(),
                        hendelse.getIdentifikator(),
                        hendelse.getGjelderPeriode()
                ))
                .collect(Collectors.toList());
    }
}
