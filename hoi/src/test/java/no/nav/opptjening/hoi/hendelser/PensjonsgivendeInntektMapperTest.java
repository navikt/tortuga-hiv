package no.nav.opptjening.hoi.hendelser;

import no.nav.opptjening.schema.PensjonsgivendeInntekt;
import no.nav.opptjening.skatt.schema.BeregnetSkatt;
import org.junit.Assert;
import org.junit.Test;

public class PensjonsgivendeInntektMapperTest {
    @Test
    public void toPensjonsgivendeInntekt() throws Exception {
        BeregnetSkatt beregnetSkatt = BeregnetSkatt.newBuilder()
                .setPersonidentifikator("12345678911")
                .setInntektsaar("2018")
                .setSumSaerfradrag(1234)
                .setSkjermingsfradrag(2345)
                .setFormuePrimaerbolig(3456)
                .setSamletGjeld(4567)
                .setPersoninntektLoenn(5678)
                .setPersoninntektPensjon(6789)
                .setPersoninntektFiskeFangstFamiliebarnehage(7890)
                .setPersoninntektNaering(8901)
                .setPersoninntektBarePensjonsdel(9012)
                .setPersoninntektBareSykedel(12345)
                .setSamletPensjon(23456)
                .setPensjonsgrad(100.1)
                .setAntallMaanederPensjon(12)
                .setTolvdeler(10)
                .setSkatteklasse("1E")
                .setNettoformue(34567)
                .setNettoinntekt(45678)
                .setUtlignetSkatt(56789)
                .setGrunnlagTrinnskatt(67890)
                .setSvalbardGjeld(78901)
                .setSvalbardLoennLoennstrekkordningen(89012)
                .setSvalbardPensjonLoennstrekkordningen(90123)
                .setSvalbardPersoninntektNaering(123456)
                .setSvalbardLoennUtenTrygdeavgiftLoennstrekkordningen(234567)
                .setSvalbardSumAllePersoninntekter(345678)
                .setSvalbardNettoformue(456789)
                .setSvalbardNettoinntekt(567890)
                .setSvalbardUtlignetSkatt(678901)
                .setSvalbardUfoeretrygdLoennstrekkordningen(789012)
                .setGrunnlagTrinnskattUtenomPersoninntekt(890123)
                .setPersoninntektUfoeretrygd(901234)
                .build();

        PensjonsgivendeInntekt pensjonsgivendeInntekt = new PensjonsgivendeInntektMapper().toPensjonsgivendeInntekt(beregnetSkatt);

        Assert.assertEquals(beregnetSkatt.getPersonidentifikator(), pensjonsgivendeInntekt.getPersonidentifikator());
        Assert.assertEquals(beregnetSkatt.getInntektsaar(), pensjonsgivendeInntekt.getInntektsaar());

        Assert.assertEquals(beregnetSkatt.getPersoninntektLoenn(), pensjonsgivendeInntekt.getFastlandsinntekt().getPersoninntektLoenn());
        Assert.assertEquals(beregnetSkatt.getPersoninntektFiskeFangstFamiliebarnehage(), pensjonsgivendeInntekt.getFastlandsinntekt().getPersoninntektFiskeFangstFamiliebarnehage());
        Assert.assertEquals(beregnetSkatt.getPersoninntektNaering(), pensjonsgivendeInntekt.getFastlandsinntekt().getPersoninntektNaering());
        Assert.assertEquals(beregnetSkatt.getPersoninntektBarePensjonsdel(), pensjonsgivendeInntekt.getFastlandsinntekt().getPersoninntektBarePensjonsdel());

        Assert.assertEquals(beregnetSkatt.getSvalbardLoennLoennstrekkordningen(), pensjonsgivendeInntekt.getSvalbardinntekt().getSvalbardLoennLoennstrekkordningen());
        Assert.assertEquals(beregnetSkatt.getSvalbardPersoninntektNaering(), pensjonsgivendeInntekt.getSvalbardinntekt().getSvalbardPersoninntektNaering());
    }

}
