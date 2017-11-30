package no.nav.opptjening.api.internal;

import no.nav.opptjening.api.hendelser.HendelseService;
import no.nav.opptjening.api.hendelser.domain.Hendelse;
import no.nav.opptjening.api.pgi.InntektService;
import no.nav.opptjening.api.pgi.domain.Inntekt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@RestController
@RequestMapping("/")
public class ScaffoldEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(ScaffoldEndpoint.class);

    private final HendelseService hendelseSerice;
    private final InntektService inntektService;

    public ScaffoldEndpoint(HendelseService hendelseSerice, InntektService inntektService) {
        this.hendelseSerice = hendelseSerice;
        this.inntektService = inntektService;
    }

    @RequestMapping(method = {RequestMethod.POST}, value="/createHendelser/{count}")
    public void createHendelser(@PathVariable int count) {
        LOG.info("Lager {} hendelser og inntekter", count);

        List<Hendelse> hendelser = new ArrayList<>(count);
        List<Inntekt> inntekter = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            String fnr = getRandomFnr();
            Hendelse hendelse = new Hendelse(fnr, "2017");
            Inntekt inntekt = new Inntekt(new Inntekt.PersonInntekt(fnr, "2017"), getRandomInntekt());

            hendelser.add(hendelse);
            inntekter.add(inntekt);
        }

        inntektService.lagreInntekter(inntekter);
        hendelseSerice.lagreHendelser(hendelser);
    }

    private int getRandomInt(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

    private double getRandomInntekt() {
        return (double)getRandomInt(1000, 1000000);
    }

    private String getRandomFnr() {
        StringBuilder sb = new StringBuilder();
        sb.append(getRandomInt(1, 31));
        sb.append(getRandomInt(1, 12));
        sb.append(getRandomInt(10, 99));
        sb.append(getRandomInt(10000, 99999));
        return sb.toString();
    }
}
