package no.nav.opptjening.hiv;

import no.nav.opptjening.schema.skatt.hendelsesliste.Hendelse;
import no.nav.opptjening.schema.skatt.hendelsesliste.HendelseKey;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;

class HendelseProducerRecordMapper {

    @NotNull
    public ProducerRecord<HendelseKey, Hendelse> mapToProducerRecord(@NotNull String topic, @NotNull Hendelse hendelse) {
        return new ProducerRecord<>(topic, HendelseKey.newBuilder()
                .setGjelderPeriode(hendelse.getGjelderPeriode())
                .setIdentifikator(hendelse.getIdentifikator())
                .build(), hendelse);
    }
}
