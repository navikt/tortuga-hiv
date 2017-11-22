package no.nav.opptjening.skatt.api.hendelser;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.stream.Collectors;

class HendelseResponsBean {

    private List<HendelseDto> hendelser;

    @JsonCreator
    HendelseResponsBean(@JsonProperty(value = "hendelser", required = true) List<HendelseBean> hendelser) {
        this.hendelser = hendelser.stream()
                .map(hendelseBean -> new HendelseDto(hendelseBean.getSekvensnummer(),
                        hendelseBean.getIdentifikator(), hendelseBean.getGjelderPeriode()))
                .collect(Collectors.toList());
    }

    List<HendelseDto> getHendelser() {
        return hendelser;
    }
}
