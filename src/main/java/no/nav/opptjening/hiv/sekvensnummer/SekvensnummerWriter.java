package no.nav.opptjening.hiv.sekvensnummer;

public interface SekvensnummerWriter {

    /**
     * persists the sekvensnummer of the next record we want to read, i.e. writeSekvensnummer(lastSekvensnummer + 1)
     * @param nextSekvensnummer
     */
    void writeSekvensnummer(long nextSekvensnummer);
}
