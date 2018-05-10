package no.nav.opptjening.hiv.sekvensnummer;

public class InMemorySekvensnummerReader implements SekvensnummerReader, SekvensnummerWriter {
    private long sekvensnummer;

    public InMemorySekvensnummerReader() {
        sekvensnummer = 1;
    }

    public long readSekvensnummer() {
        return sekvensnummer;
    }

    public void writeSekvensnummer(long nextSekvensnummer) {
        sekvensnummer = nextSekvensnummer;
    }
}
