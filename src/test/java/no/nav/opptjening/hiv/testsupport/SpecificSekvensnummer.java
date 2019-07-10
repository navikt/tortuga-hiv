package no.nav.opptjening.hiv.testsupport;

import no.nav.opptjening.hiv.sekvensnummer.SekvensnummerReader;

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