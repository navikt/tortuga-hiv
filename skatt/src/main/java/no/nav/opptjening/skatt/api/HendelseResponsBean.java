package no.nav.opptjening.skatt.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import no.nav.opptjening.skatt.dto.HendelseDto;

import java.util.List;
import java.util.stream.Collectors;

class HendelseResponsBean {

    private List<HendelseBean> hendelser;

    @JsonCreator
    HendelseResponsBean(@JsonProperty(value = "hendelser", required = true) List<HendelseBean> hendelser) {
        this.hendelser = hendelser;
    }

    List<HendelseDto> getHendelser() {
        return hendelser.stream()
                .map(h -> new HendelseDto(h.getSekvensnummer(), h.getIdentifikator(), h.getGjelderPeriode()))
                .collect(Collectors.toList());
    }
}
