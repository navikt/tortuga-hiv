package no.nav.opptjening.hiv.sekvensnummer;

import java.util.Optional;

public interface SekvensnummerReader {

    /**
     * @return the next sekvensnummer to start reading from.
     * consecutive calls to read() yields the same number
     */
    Optional<Long> readSekvensnummer();
}
