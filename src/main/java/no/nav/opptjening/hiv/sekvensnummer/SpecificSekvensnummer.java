package no.nav.opptjening.hiv.sekvensnummer;

import java.time.LocalDate;
import java.util.function.Supplier;

import no.nav.opptjening.skatt.client.api.hendelseliste.HendelserClient;

/**
 * Only to be used temporarily to initialize read of records from a specific sekvensnummer.
 */
public class SpecificSekvensnummer extends Sekvensnummer {

    //First sekvensnummer of 2018 events
    private final long specificSekvensnummer = 11745289L;

    public SpecificSekvensnummer(HendelserClient beregnetskattHendelserClient, SekvensnummerReader sekvensnummerReader, Supplier<LocalDate> dateSupplier) {
        super(beregnetskattHendelserClient, sekvensnummerReader, dateSupplier);
    }

    /**
     * Override to allow reading from specific sekvensnummer in the dirtiest way possible... if need be.
     * Beware; reading will always continue from {@link #specificSekvensnummer} upon pod-restart.
     */
    @Override
    protected Long initNextSekvensnummer() {
        nextSekvensnummerGauge.set(specificSekvensnummer);
        return specificSekvensnummer;
    }
}
