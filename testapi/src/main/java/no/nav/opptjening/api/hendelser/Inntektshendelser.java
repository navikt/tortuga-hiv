package no.nav.opptjening.api.hendelser;

import no.nav.opptjening.dto.InntektHendelseDto;
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

    public Inntektshendelser(CounterBuffers counterBuffers) {
        this.counterBuffers = counterBuffers;
    }

    @RequestMapping(method = RequestMethod.GET)
    public List<InntektHendelseDto> lagListeMedVarslinger(@RequestParam(value = "siden", required = false) @DateTimeFormat(pattern="yyyy-MM-dd") Date dato) {
        List<InntektHendelseDto> varsel = InntektHendelseFactory.createList(dato);

        counterBuffers.increment("counter.inntektvarsel", (long)varsel.size());

        return varsel;
    }
}
