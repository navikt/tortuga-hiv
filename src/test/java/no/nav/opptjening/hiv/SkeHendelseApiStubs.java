package no.nav.opptjening.hiv;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import no.nav.opptjening.skatt.client.Hendelsesliste;
import no.nav.opptjening.skatt.client.schema.hendelsesliste.HendelseslisteDto;

import java.time.LocalDate;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/* Stubs https://skatteetaten.github.io/datasamarbeid-api-dokumentasjon/reference_feed */
class SkeHendelseApiStubs {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String HENDELSER_START_URL = "/hendelser/start";
    private static final int FIRST_SEKVENSNUMMER_FROM_SKE = 1;
    private static final String HENDELSER_URL = "/hendelser/";
    private static final String API_KEY = "apikey";
    private static final String NAV_API_KEY_HEADER = "X-Nav-Apikey";
    private static final String DATO_QUERY_PARAM = "dato";
    private static final String FRA_SEKVENSNUMMER_QUERY_PARAM = "fraSekvensnummer";
    private static final String ANTALL_QUERY_PARAM = "antall";

    static void stub400statusCodesFromSkatteEtaten(LocalDate specificDate) {
        WireMock.stubFor(WireMock.get(WireMock.urlPathEqualTo(HENDELSER_START_URL))
                .withQueryParam(DATO_QUERY_PARAM, WireMock.equalTo(specificDate.toString()))
                .withHeader(NAV_API_KEY_HEADER, WireMock.equalTo(API_KEY))
                .willReturn(WireMock.badRequest()
                        .withBody("this is not valid json")
                        .withHeader("Content-type", "application/json")));
    }

    static void stub500statusCodesFromSkatteEtaten(LocalDate specificDate) {
        WireMock.stubFor(WireMock.get(WireMock.urlPathEqualTo(HENDELSER_START_URL))
                .withQueryParam(DATO_QUERY_PARAM, WireMock.equalTo(specificDate.toString()))
                .withHeader(NAV_API_KEY_HEADER, WireMock.equalTo(API_KEY))
                .willReturn(WireMock.serverError()
                        .withBody("internal server error")));
    }

    static void stubFirstSekvensnummerFromSkatteEtaten() {
        WireMock.stubFor(WireMock.get(WireMock.urlEqualTo(HENDELSER_START_URL))
                .withHeader(NAV_API_KEY_HEADER, WireMock.equalTo(API_KEY))
                .willReturn(WireMock.okJson(getSekvensnummerJson(FIRST_SEKVENSNUMMER_FROM_SKE))));
    }

    static void stubSekvensnummerLimit(int sekvensnummerLimit, LocalDate specificDate) {
        WireMock.stubFor(WireMock.get(WireMock.urlPathEqualTo(HENDELSER_START_URL))
                .withQueryParam(DATO_QUERY_PARAM, WireMock.equalTo(specificDate.toString()))
                .withHeader(NAV_API_KEY_HEADER, WireMock.equalTo(API_KEY))
                .willReturn(WireMock.okJson(getSekvensnummerJson(sekvensnummerLimit))));
    }

    static void stubHendelser(String fraSekvensnummer, int antallHendelserPerRequest, String jsonHendelse) {
        WireMock.stubFor(WireMock.get(WireMock.urlPathEqualTo(HENDELSER_URL))
                .withQueryParam(FRA_SEKVENSNUMMER_QUERY_PARAM, WireMock.equalTo(fraSekvensnummer))
                .withQueryParam(ANTALL_QUERY_PARAM, WireMock.equalTo(String.valueOf(antallHendelserPerRequest)))
                .withHeader(NAV_API_KEY_HEADER, WireMock.equalTo(API_KEY))
                .willReturn(WireMock.okJson(jsonHendelse)));
    }

    static void stubHendelseFeed(int fromSekvensnummer, int amountHendelserPerRequest, List<HendelseslisteDto.HendelseDto> mockHendelser) throws Exception {
        stubHendelse(fromSekvensnummer++, amountHendelserPerRequest, mockHendelser, 0);
        stubHendelse(fromSekvensnummer++, amountHendelserPerRequest, mockHendelser, 1);
        stubHendelse(fromSekvensnummer, amountHendelserPerRequest, mockHendelser, 2);
    }

    private static void stubHendelse(int fraSekvensnummer, int antallHendelserPerRequest, List<HendelseslisteDto.HendelseDto> mockHendelser, int index) throws Exception {
        WireMock.stubFor(WireMock.get(WireMock.urlPathEqualTo(HENDELSER_URL))
                .withQueryParam(FRA_SEKVENSNUMMER_QUERY_PARAM, WireMock.equalTo(String.valueOf(fraSekvensnummer)))
                .withQueryParam(ANTALL_QUERY_PARAM, WireMock.equalTo(String.valueOf(antallHendelserPerRequest)))
                .withHeader(NAV_API_KEY_HEADER, WireMock.equalTo(API_KEY))
                .willReturn(WireMock.okJson(getJsonHendelse(mockHendelser.get(index)))));
    }

    static String getJsonMockHendelser(List<HendelseslisteDto.HendelseDto> mockHendelser) throws Exception {
        Map<String, List<HendelseslisteDto.HendelseDto>> response = new HashMap<>();
        response.put("hendelser", mockHendelser);
        return OBJECT_MAPPER.writeValueAsString(response);
    }

    static void assertHendelser(List<HendelseslisteDto.HendelseDto> expected, List<Hendelsesliste.Hendelse> actual) {
        assertEquals(expected.size(), actual.size());
        IntStream.range(0, actual.size()).forEach(index -> assertHendelse(expected.get(index), actual.get(index)));
    }

    static void assertHendelse(HendelseslisteDto.HendelseDto expected, Hendelsesliste.Hendelse actual) {
        assertEquals(expected.getSekvensnummer(), actual.getSekvensnummer());
        assertEquals(expected.getIdentifikator(), actual.getIdentifikator());
        assertEquals(expected.getGjelderPeriode(), actual.getGjelderPeriode());
    }

    private static String getJsonHendelse(HendelseslisteDto.HendelseDto hendelse) throws Exception {
        return getJsonMockHendelser(Collections.singletonList(hendelse));
    }

    private static String getSekvensnummerJson(int sekvensnummerLimit) {
        return "{\"sekvensnummer\": " + sekvensnummerLimit + "}";
    }
}
