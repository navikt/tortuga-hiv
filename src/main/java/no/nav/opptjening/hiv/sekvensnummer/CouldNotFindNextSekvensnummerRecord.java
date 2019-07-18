package no.nav.opptjening.hiv.sekvensnummer;

public class CouldNotFindNextSekvensnummerRecord extends RuntimeException {
    CouldNotFindNextSekvensnummerRecord(String s) {
        super(s);
    }
}
