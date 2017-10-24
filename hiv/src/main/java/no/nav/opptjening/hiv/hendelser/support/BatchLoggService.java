package no.nav.opptjening.hiv.hendelser.support;

public class BatchLoggService {

    private BatchLoggRepository repository;

    public BatchLoggService(BatchLoggRepository repository) {
        this.repository = repository;
    }

    public BatchLoggEntry findLatest() {
        return repository.findTop1ByOrderByJobIdDesc();
    }

    public void createNew(long jobId, long fraSekvensnummer, long tilSekvensnummer) {

        BatchLoggEntry entry = repository.findOne(jobId);

        if (entry == null) {
            entry = new BatchLoggEntry(jobId, fraSekvensnummer, tilSekvensnummer);
        } else {
            assert fraSekvensnummer == entry.getFraSekvensnummer();
            entry.setTilSekvensnummer(tilSekvensnummer);
        }

        repository.save(entry);
    }
}