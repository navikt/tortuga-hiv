package no.nav.opptjening.api.hendelser;

import no.nav.opptjening.skatt.dto.InntektHendelseDto;
import org.springframework.boot.actuate.metrics.buffer.CounterBuffers;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.List;

@RestController
@RequestMapping("/hendelser")
public class Inntektshendelser {

    private final CounterBuffers counterBuffers;

    private HendelseService service;

    public Inntektshendelser(HendelseService service, CounterBuffers counterBuffers) {
        this.service = service;
        this.counterBuffers = counterBuffers;
    }

    @RequestMapping(method = RequestMethod.GET)
    public List<InntektHendelseDto> hentVarsel(@RequestParam(value = "siden", required = false) @DateTimeFormat(pattern="yyyy-MM") Date dato) {
        List<InntektHendelseDto> varsel;

        if (dato == null) {
            varsel = service.hentHendelser();
        } else {
            varsel = service.hentHendelserEtter(dato);
        }

        counterBuffers.increment("counter.inntektvarsel", (long)varsel.size());

        return varsel;
    }
}
