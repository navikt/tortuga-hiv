package no.nav.opptjening.hoi.hendelser;

import no.nav.opptjening.schema.PensjonsgivendeInntekt;
import no.nav.opptjening.schema.skatteetaten.BeregnetSkatt;

public class PensjonsgivendeInntektMapper {

    public PensjonsgivendeInntekt toPensjonsgivendeInntekt(BeregnetSkatt beregnetSkatt) {
        return PensjonsgivendeInntekt.newBuilder()
                .setPersonidentifikator(beregnetSkatt.getPersonidentifikator())
                .setInntektsaar(beregnetSkatt.getInntektsaar())
                .setPersoninntektBarePensjonsdel(beregnetSkatt.getPersoninntektBarePensjonsdel())
                .setPersoninntektLoenn(beregnetSkatt.getPersoninntektLoenn())
                .setPersoninntektNaering(beregnetSkatt.getPersoninntektNaering())
                .setPersoninntektFiskeFangstFamiliebarnehage(beregnetSkatt.getPersoninntektFiskeFangstFamiliebarnehage())
                .setSvalbardLoennLoennstrekkordningen(beregnetSkatt.getSvalbardLoennLoennstrekkordningen())
                .setSvalbardPersoninntektNaering(beregnetSkatt.getSvalbardPersoninntektNaering())
                .build();
    }
}
