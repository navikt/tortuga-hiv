package no.nav.opptjening.hiv.sekvensnummer;

import java.util.Optional;

public class SpecificSekvensnummer implements SekvensnummerReader {
        private final Long sekvensnummer;

        public SpecificSekvensnummer(Long sekvensnummer) {
            this.sekvensnummer = sekvensnummer;
        }

        @Override
        public Optional<Long> readSekvensnummer() {
            return Optional.ofNullable(sekvensnummer);
        }
    }