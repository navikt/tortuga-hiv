package no.nav.opptjening.hiv.testsupport;

import no.nav.opptjening.hiv.sekvensnummer.SekvensnummerReader;

import java.util.Optional;

public class SpecificSekvensnummerReader implements SekvensnummerReader {
        private final Long sekvensnummer;

        public SpecificSekvensnummerReader(Long sekvensnummer) {
            this.sekvensnummer = sekvensnummer;
        }

        @Override
        public Optional<Long> readSekvensnummer() {
            return Optional.ofNullable(sekvensnummer);
        }
    }