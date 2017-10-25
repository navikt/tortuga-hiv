package no.nav.opptjening.api.hendelser;

import java.util.List;
import java.util.stream.Collectors;

class HendelseResponsDto {

    private List<HendelseDto> hendelser;

    HendelseResponsDto(List<HendelseDto> hendelser) {
        this.hendelser = hendelser;
    }

    public List<HendelseDto> getHendelser() {
        return hendelser.stream()
                .map(h -> new HendelseDto(h.getSekvensnummer(), h.getIdentifikator(), h.getGjelderPeriode()))
                .collect(Collectors.toList());
    }
}
