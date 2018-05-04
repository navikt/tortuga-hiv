package no.nav.opptjening.skatt.api.beregnetskatt;

import no.nav.opptjening.skatt.api.exceptions.ResponseUnmappableException;
import no.nav.opptjening.skatt.api.exceptions.UkjentFeilkodeException;
import no.nav.opptjening.skatt.exceptions.*;
import no.nav.opptjening.skatt.schema.BeregnetSkatt;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.fail;

public class BeregnetSkattClientTest {
    @Rule
    public MockWebServer server = new MockWebServer();

    private BeregnetSkattClient beregnetSkattClient;

    @Before
    public void setUp() throws Exception {
        this.beregnetSkattClient = new BeregnetSkattClient(server.url("/").toString());
    }

    @Test
    public void when_ResponseIsOk_Then_CorrectValuesAreMappedOk() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("{\n" +
                        "    \"personidentifikator\": \"12345678901\",\n" +
                        "    \"inntektsaar\": \"2016\",\n" +
                        "    \"sumSaerfradrag\": 80000,\n" +
                        "    \"skjermingsfradrag\": 1000,\n" +
                        "    \"formuePrimaerbolig\": 2000000,\n" +
                        "    \"samletGjeld\": 50000,\n" +
                        "    \"personinntektLoenn\": 490000,\n" +
                        "    \"personinntektPensjon\": 100000,\n" +
                        "    \"personinntektFiskeFangstFamiliebarnehage\": 90000,\n" +
                        "    \"personinntektNaering\": 70000,\n" +
                        "    \"personinntektBarePensjonsdel\": 40000,\n" +
                        "    \"personinntektBareSykedel\": 30000,\n" +
                        "    \"samletPensjon\": 10700,\n" +
                        "    \"pensjonsgrad\": 100.1,\n" +
                        "    \"antallMaanederPensjon\": 12,\n" +
                        "    \"tolvdeler\": 12,\n" +
                        "    \"skatteklasse\": \"1E\",\n" +
                        "    \"nettoformue\": 300000,\n" +
                        "    \"nettoinntekt\": 400000,\n" +
                        "    \"utlignetSkatt\": 100000,\n" +
                        "    \"grunnlagTrinnskatt\": 500000,\n" +
                        "    \"svalbardGjeld\": 234567,\n" +
                        "    \"svalbardLoennLoennstrekkordningen\": 123456,\n" +
                        "    \"svalbardPensjonLoennstrekkordningen\": 123456,\n" +
                        "    \"svalbardPersoninntektNaering\": 123456,\n" +
                        "    \"svalbardLoennUtenTrygdeavgiftLoennstrekkordningen\": 123456,\n" +
                        "    \"svalbardSumAllePersoninntekter\": 1234567,\n" +
                        "    \"svalbardNettoformue\": 123456,\n" +
                        "    \"svalbardNettoinntekt\": 123456,\n" +
                        "    \"svalbardUtlignetSkatt\": 34567,\n" +
                        "    \"svalbardUfoeretrygdLoennstrekkordningen\": 4564,\n" +
                        "    \"grunnlagTrinnskattUtenomPersoninntekt\": 324231,\n" +
                        "    \"personinntektUfoeretrygd\": 32232\n" +
                        "}\n" +
                        "\n")
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
        );

        BeregnetSkatt result = beregnetSkattClient.getBeregnetSkatt("nav","2016", "12345678901");

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/nav/2016/12345678901", request.getPath());
        Assert.assertEquals("GET", request.getMethod());

        Assert.assertEquals("12345678901", result.getPersonidentifikator());
        Assert.assertEquals("2016", result.getInntektsaar());
        Assert.assertEquals(80000, result.getSumSaerfradrag(), 0);
        Assert.assertEquals(1000, result.getSkjermingsfradrag(), 0);
        Assert.assertEquals(2000000, result.getFormuePrimaerbolig(), 0);
        Assert.assertEquals(50000, result.getSamletGjeld(), 0);
        Assert.assertEquals(490000, result.getPersoninntektLoenn(), 0);
        Assert.assertEquals(100000, result.getPersoninntektPensjon(), 0);
        Assert.assertEquals(90000, result.getPersoninntektFiskeFangstFamiliebarnehage(), 0);
        Assert.assertEquals(70000, result.getPersoninntektNaering(), 0);
        Assert.assertEquals(40000, result.getPersoninntektBarePensjonsdel(), 0);
        Assert.assertEquals(30000, result.getPersoninntektBareSykedel(), 0);
        Assert.assertEquals(10700, result.getSamletPensjon(), 0);
        Assert.assertEquals("100.1", Double.toString(result.getPensjonsgrad()));
        Assert.assertEquals(12, result.getAntallMaanederPensjon(), 0);
        Assert.assertEquals(12, result.getTolvdeler(), 0);
        Assert.assertEquals("1E", result.getSkatteklasse());
        Assert.assertEquals(300000, result.getNettoformue(), 0);
        Assert.assertEquals(400000, result.getNettoinntekt(), 0);
        Assert.assertEquals(100000, result.getUtlignetSkatt(), 0);
        Assert.assertEquals(500000, result.getGrunnlagTrinnskatt(), 0);
        Assert.assertEquals(234567, result.getSvalbardGjeld(), 0);
        Assert.assertEquals(123456, result.getSvalbardLoennLoennstrekkordningen(), 0);
        Assert.assertEquals(123456, result.getSvalbardPensjonLoennstrekkordningen(), 0);
        Assert.assertEquals(123456, result.getSvalbardPersoninntektNaering(), 0);
        Assert.assertEquals(123456, result.getSvalbardLoennUtenTrygdeavgiftLoennstrekkordningen(), 0);
        Assert.assertEquals(1234567, result.getSvalbardSumAllePersoninntekter(), 0);
        Assert.assertEquals(123456, result.getSvalbardNettoformue(), 0);
        Assert.assertEquals(123456, result.getSvalbardNettoinntekt(), 0);
        Assert.assertEquals(34567, result.getSvalbardUtlignetSkatt(), 0);
        Assert.assertEquals(4564, result.getSvalbardUfoeretrygdLoennstrekkordningen(), 0);
        Assert.assertEquals(324231, result.getGrunnlagTrinnskattUtenomPersoninntekt(), 0);
        Assert.assertEquals(32232, result.getPersoninntektUfoeretrygd(), 0);
    }

    @Test
    public void when_ResponseCannotBeMappedToAvroSchema_Then_Throw() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("{\"pensjonsgivendeInntekt\": 150000}")
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
        );

        try {
            beregnetSkattClient.getBeregnetSkatt("nav","2016", "12345");
            fail("Expected an ResponseUnmappableException to be thrown");
        } catch (ResponseUnmappableException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/nav/2016/12345", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test
    public void when_ResponseFailedWithFeilmelding_Then_ThrowMappedException() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("{\"kode\": \"BSA-005\", \"melding\": \"Det forespurte inntektsåret er ikke støttet\"}")
                .setResponseCode(400)
                .addHeader("Content-Type", "application/json")
        );

        try {
            beregnetSkattClient.getBeregnetSkatt("nav","2016", "12345");
            fail("Expected an BadRequestException to be thrown");
        } catch (BadRequestException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/nav/2016/12345", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test
    public void when_ReponseFailedWithUnknownFeilkode_Then_ThrowUkjentFeilkodeException() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("{\"kode\": \"ZZ-001\", \"melding\": \"Denne feilkoden er ikke implementert\"}")
                .setResponseCode(400)
                .addHeader("Content-Type", "application/json")
        );

        try {
            beregnetSkattClient.getBeregnetSkatt("nav","2016", "12345");
            fail("Expected an UkjentFeilkodeException to be thrown");
        } catch (UkjentFeilkodeException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/nav/2016/12345", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test
    public void when_ResponseFailedWithGeneric4xxError_Then_ThrowClientException() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("bad request")
                .setResponseCode(400)
                .addHeader("Content-Type", "text/plan")
        );

        try {
            beregnetSkattClient.getBeregnetSkatt("nav","2016", "12345");
            fail("Expected an ClientException to be thrown");
        } catch (ClientException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/nav/2016/12345", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test
    public void when_ResponseFailedWithGeneric5xxError_Then_ThrowServerException() throws Exception {
        server.enqueue(new MockResponse()
                .setBody("internal server error")
                .setResponseCode(500)
                .addHeader("Content-Type", "text/plan")
        );

        try {
            beregnetSkattClient.getBeregnetSkatt("nav","2016", "12345");
            fail("Expected an ServerException to be thrown");
        } catch (ServerException e) {
            // ok
        }

        RecordedRequest request = server.takeRequest();
        Assert.assertEquals("/nav/2016/12345", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }
}
