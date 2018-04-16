package no.nav.opptjening.hiv.hendelser;

public class SekvensnummerService {

    private SekvensnummerStorage storage;

    public SekvensnummerService(SekvensnummerStorage storage) {
        this.storage = storage;
    }

    public long getSekvensnummer() {
        return storage.getSekvensnummer();
    }

    public void persistSekvensnummer(long sekvensnummer) {
        storage.persistSekvensnummer(sekvensnummer);
    }
}
