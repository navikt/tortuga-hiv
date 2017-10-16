package no.nav.opptjening.api.hendelser;

import no.nav.opptjening.api.hendelser.repository.InntektsfeedRepository;
import no.nav.opptjening.skatt.dto.InntektHendelseDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class HendelseService {

    private static final Logger LOG = LoggerFactory.getLogger(HendelseService.class);

    private InntektsfeedRepository repository;

    public HendelseService(InntektsfeedRepository repository) {
        this.repository = repository;
    }

    public List<InntektHendelseDto> hentHendelser() {
        LOG.info("Henter alle hendelser");

        return repository.findAll().stream()
                .map(inntektHendelse -> {
                    InntektHendelseDto hendelseDto = new InntektHendelseDto();
                    hendelseDto.personindentfikator = inntektHendelse.fnr;
                    hendelseDto.inntektsaar = inntektHendelse.gjelderPeriode;
                    hendelseDto.endret = inntektHendelse.dokumenttilstand;
                    return hendelseDto;
                })
                .collect(Collectors.toList());
    }

    public List<InntektHendelseDto> hentHendelserEtter(Date etterDato) {
        Format formatter = new SimpleDateFormat("yyyy-MM");
        String dato = formatter.format(etterDato);

        LOG.info("Henter hendelser etter {}", dato);

        return repository.findByGjelderPeriodeAfter(dato).stream()
                .map(inntektHendelse -> {
                    InntektHendelseDto hendelseDto = new InntektHendelseDto();
                    hendelseDto.personindentfikator = inntektHendelse.fnr;
                    hendelseDto.inntektsaar = inntektHendelse.gjelderPeriode;
                    hendelseDto.endret = inntektHendelse.dokumenttilstand;
                    return hendelseDto;
                })
                .collect(Collectors.toList());
    }
}
