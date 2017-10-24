package no.nav.opptjening.hiv.hendelser.support;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "BATCH_LOGG")
public class BatchLoggEntry {

    @Id
    private long jobId;

    @Column
    private long fraSekvensnummer;

    @Column
    private long tilSekvensnummer;

    protected BatchLoggEntry() {}

    BatchLoggEntry(long jobId, long fraSekvensnummer, long tilSekvensnummer) {
        this.jobId = jobId;
        this.fraSekvensnummer = fraSekvensnummer;
        this.tilSekvensnummer = tilSekvensnummer;
    }

    long getJobId() {
        return jobId;
    }

    long getFraSekvensnummer() {
        return fraSekvensnummer;
    }

    public long getTilSekvensnummer() {
        return tilSekvensnummer;
    }

    void setTilSekvensnummer(long tilSekvensnummer) {
        this.tilSekvensnummer = tilSekvensnummer;
    }

    @Override
    public String toString() {
        return String.format("[jobId=%d, fraSekvensnummer=%s, tilSekvensnummer=%s]",
                jobId, fraSekvensnummer, tilSekvensnummer);
    }
}
