package no.nav.opptjening.hiv.hendelser;

public interface SekvensnummerStorage {

    long getSekvensnummer();

    void persistSekvensnummer(long sekvensnummer);
}
