package no.nav.opptjening.api.hendelser;

import no.nav.opptjening.api.ApiException;
import no.nav.opptjening.skatt.dto.HendelseDto;
import no.nav.opptjening.skatt.dto.SekvensDto;
import org.springframework.boot.actuate.metrics.buffer.CounterBuffers;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.xml.ws.http.HTTPException;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.Random;

@RestController
@RequestMapping("/api/formueinntekt/beregnetskatt/hendelser")
public class Hendelser {

    private final CounterBuffers counterBuffers;

    private HendelseService service;

    private static final int DEFAULT_READ_SIZE = 100;

    private Random random = new Random();

    public Hendelser(HendelseService service, CounterBuffers counterBuffers) {
        this.service = service;
        this.counterBuffers = counterBuffers;
    }

    @RequestMapping(method = RequestMethod.GET)
    public HendelseRespons hentHendelser(
            @RequestParam(value = "fraSekvensnummer") Optional<Integer> sekvens,
            @RequestParam(value = "antall") Optional<Integer> antall) {
        int seq = sekvens.orElseThrow(() -> new ApiException(HttpStatus.BAD_REQUEST, "FA-001", "fraSekvensnummer må være satt"));
        if (seq <= 0) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "FA-002", "fraSekvensnummer må være større enn 0");
        }

        int ant = antall.orElseThrow(() -> new ApiException(HttpStatus.BAD_REQUEST, "FA-003", "antall må være spesifisert"));
        if (ant <= 0) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "FA-004", "antall må være større enn 0");
        }

        int randomNumber = random.nextInt(50);
        // simulate error that does not map to a valid FeilmeldingDto json format
        if (randomNumber == 0) {
            throw new HTTPException(400);
        }
        // simulate error that does not map to a valid ApiException
        if (randomNumber == 1) {
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "ZZ-999", "Dette er ikke en gyldig feilmeldingskode, men den har riktig JSON format");
        }

        // simulate db error
        if (randomNumber == 2) {
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "FA-007", "Den forespurte informasjonen er for øyeblikket utilgjengelig, " +
                    "vennligst prøv igjen senere! Dersom problemet vedvarer, ta kontakt med brukerstøtte!");
        }

        if (randomNumber == 3) {
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "FA-009", "Fikk ikke hentet data fra databasen, vennligst prøv igjen senere!");
        }

        List<HendelseDto> hendelser = service.hentHendelser(seq, ant);

        if (hendelser.size() == 0) {
            throw new ApiException(HttpStatus.NOT_FOUND, "FA-010", "Fant ingen informasjon i databasen");
        }

        counterBuffers.increment("counter.hendelser", (long)hendelser.size());

        return new HendelseRespons(hendelser);
    }

    @RequestMapping(value = "/start", method = RequestMethod.GET)
    public SekvensDto hentSisteSekvens(@RequestParam(value = "dato") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dato) {
        SekvensDto sekvensDto = new SekvensDto(1);

        if (sekvensDto == null) {
            throw new ApiException(HttpStatus.NOT_FOUND, "FA-014", "Fant ingen sekvensnummer.");
        }

        return sekvensDto;
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
