package no.nav.opptjening.hoi.hendelser;

import no.nav.opptjening.schema.Fastlandsinntekt;
import no.nav.opptjening.schema.PensjonsgivendeInntekt;
import no.nav.opptjening.schema.Svalbardinntekt;
import no.nav.opptjening.skatt.schema.BeregnetSkatt;

public class PensjonsgivendeInntektMapper {

    public PensjonsgivendeInntekt toPensjonsgivendeInntekt(BeregnetSkatt beregnetSkatt) {
        return PensjonsgivendeInntekt.newBuilder()
                .setPersonidentifikator(beregnetSkatt.getPersonidentifikator())
                .setInntektsaar(beregnetSkatt.getInntektsaar())
                .setFastlandsinntekt(Fastlandsinntekt.newBuilder()
                    .setPersoninntektBarePensjonsdel(beregnetSkatt.getPersoninntektBarePensjonsdel())
                    .setPersoninntektLoenn(beregnetSkatt.getPersoninntektLoenn())
                    .setPersoninntektNaering(beregnetSkatt.getPersoninntektNaering())
                    .setPersoninntektFiskeFangstFamiliebarnehage(beregnetSkatt.getPersoninntektFiskeFangstFamiliebarnehage())
                    .build())
                .setSvalbardinntekt(Svalbardinntekt.newBuilder()
                    .setSvalbardLoennLoennstrekkordningen(beregnetSkatt.getSvalbardLoennLoennstrekkordningen())
                    .setSvalbardPersoninntektNaering(beregnetSkatt.getSvalbardPersoninntektNaering())
                    .build())
                .build();
    }
}
