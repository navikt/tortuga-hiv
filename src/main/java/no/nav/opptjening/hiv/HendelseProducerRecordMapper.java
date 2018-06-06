package no.nav.opptjening.hiv;

import no.nav.opptjening.schema.skatt.hendelsesliste.Hendelse;
import no.nav.opptjening.skatt.client.Hendelsesliste;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;

public class HendelseProducerRecordMapper {

    private final HendelseMapper hendelseMapper = new HendelseMapper();

    @NotNull
    public ProducerRecord<String, Hendelse> mapToProducerRecord(@NotNull String topic, @NotNull Hendelsesliste.Hendelse hendelse) {
        Hendelse hendelseRecord = hendelseMapper.mapToHendelse(hendelse);
        return new ProducerRecord<>(topic, hendelseRecord.getGjelderPeriode() + "-" + hendelseRecord.getIdentifikator(), hendelseRecord);
    }
}
