package no.nav.opptjening.hiv.hendelser;

public interface SekvensnummerReader {

    /**
     * @return the next sekvensnummer to start reading from.
     * consecutive calls to read() yields the same number
     */
    long readSekvensnummer();
}
