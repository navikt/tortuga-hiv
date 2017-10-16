package no.nav.opptjening.api.hendelser;

import no.nav.opptjening.skatt.dto.HendelseDto;
import no.nav.opptjening.skatt.dto.SekvensDto;
import org.springframework.boot.actuate.metrics.buffer.CounterBuffers;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/formueinntekt/beregnetskatt/hendelser")
public class Hendelser {

    private final CounterBuffers counterBuffers;

    private HendelseService service;

    private static final int DEFAULT_READ_SIZE = 100;

    public Hendelser(HendelseService service, CounterBuffers counterBuffers) {
        this.service = service;
        this.counterBuffers = counterBuffers;
    }

    @RequestMapping(method = RequestMethod.GET)
    public HendelseRespons hentHendelser(
            @RequestParam(value = "fraSekvensnummer") Optional<Integer> sekvens,
            @RequestParam(value = "antall") Optional<Integer> antall) {

        List<HendelseDto> hendelser = service.hentHendelser(sekvens.orElse(0), antall.orElse(DEFAULT_READ_SIZE));
        counterBuffers.increment("counter.hendelser", (long)hendelser.size());

        return new HendelseRespons(hendelser);
    }

    @RequestMapping(value = "/start", method = RequestMethod.GET)
    public SekvensDto hentSisteSekvens(@RequestParam(value = "dato") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dato) {
        return new SekvensDto(1);
    }

    private static class HendelseRespons {

        private List<HendelseDto> hendelser;

        public HendelseRespons(List<HendelseDto> hendelser) {
            this.hendelser = hendelser;
        }

        public List<HendelseDto> getHendelser() {
            return hendelser;
        }
    }
}
