package no.nav.opptjening.hiv;

import no.nav.opptjening.schema.skatt.hendelsesliste.Hendelse;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;

public class HendelseProducerRecordMapper {

    @NotNull
    public ProducerRecord<String, Hendelse> mapToProducerRecord(@NotNull String topic, @NotNull Hendelse hendelse) {
        return new ProducerRecord<>(topic, hendelse.getGjelderPeriode() + "-" + hendelse.getIdentifikator(), hendelse);
    }
}
