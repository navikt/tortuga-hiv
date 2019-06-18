package no.nav.opptjening.hiv;

import no.nav.opptjening.hiv.sekvensnummer.SekvensnummerWriter;
import no.nav.opptjening.schema.skatt.hendelsesliste.Hendelse;
import no.nav.opptjening.schema.skatt.hendelsesliste.HendelseKey;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import static org.junit.jupiter.api.Assertions.*;

class SkatteoppgjorhendelseProducerTest {
    private MockProducer<HendelseKey, Hendelse> producer;
    private DummySekvensnummerWriter writer;

    private final String topic = "my-test-topic";
    private final static String EARLIEST_VALID_HENDELSE_YEAR = "2017";

    @BeforeEach
    void setUp() {
        producer = new MockProducer<>();
        writer = new DummySekvensnummerWriter();
    }

    private void waitForCondition(Callable<Boolean> callable) {
        long timeStart = System.currentTimeMillis();
        try {
            while (!callable.call()) {
                long now = System.currentTimeMillis();

                if ((now - timeStart) > 250) {
                    throw new RuntimeException("Waited for 250 milliseconds for callable to be true.");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void that_SekvensnummerIsWritten_When_RecordsAreSentOk() {
        List<Hendelse> hendelseList = Arrays.asList(
                new Hendelse(1L, "123456789", "2018"),
                new Hendelse(2L, "234567890", "2018"),
                new Hendelse(3L, "345678901", "2018"),
                new Hendelse(4L, "456789012", "2018"),
                new Hendelse(5L, "567890123", "2018")
        );

        SkatteoppgjorhendelseProducer skatteoppgjorhendelseProducer = new SkatteoppgjorhendelseProducer(producer, topic, writer, EARLIEST_VALID_HENDELSE_YEAR);
        assertEquals((long)hendelseList.get(hendelseList.size() - 1).getSekvensnummer(), skatteoppgjorhendelseProducer.sendHendelser(hendelseList));

        producer.flush();

        List<ProducerRecord<HendelseKey, Hendelse>> history = producer.history();
        List<ProducerRecord<HendelseKey, Hendelse>> expected = Arrays.asList(
                new ProducerRecord<>(topic, HendelseKey.newBuilder()
                        .setGjelderPeriode("2018")
                        .setIdentifikator("123456789")
                        .build(), hendelseList.get(0)),
                new ProducerRecord<>(topic, HendelseKey.newBuilder()
                        .setGjelderPeriode("2018")
                        .setIdentifikator("234567890")
                        .build(), hendelseList.get(1)),
                new ProducerRecord<>(topic, HendelseKey.newBuilder()
                        .setGjelderPeriode("2018")
                        .setIdentifikator("345678901")
                        .build(), hendelseList.get(2)),
                new ProducerRecord<>(topic, HendelseKey.newBuilder()
                        .setGjelderPeriode("2018")
                        .setIdentifikator("456789012")
                        .build(), hendelseList.get(3)),
                new ProducerRecord<>(topic, HendelseKey.newBuilder()
                        .setGjelderPeriode("2018")
                        .setIdentifikator("567890123")
                        .build(), hendelseList.get(4))
        );

        assertEquals(expected, history);

        assertEquals(hendelseList.get(hendelseList.size() - 1).getSekvensnummer() + 1, writer.lastWrittenSekvensnummer);
    }

    @Test
    void that_SekvensnummerIsNotWritten_When_RecordsAreNotSentOk() {
        List<Hendelse> hendelseList = Arrays.asList(
                new Hendelse(1L, "123456789", "2018"),
                new Hendelse(2L, "234567890", "2018"),
                new Hendelse(3L, "345678901", "2018"),
                new Hendelse(4L, "456789012", "2018"),
                new Hendelse(5L, "567890123", "2018")
        );

        SkatteoppgjorhendelseProducer skatteoppgjorhendelseProducer = new SkatteoppgjorhendelseProducer(producer, topic, writer, EARLIEST_VALID_HENDELSE_YEAR);
        assertEquals((long)hendelseList.get(hendelseList.size() - 1).getSekvensnummer(), skatteoppgjorhendelseProducer.sendHendelser(hendelseList));

        producer.completeNext();
        producer.errorNext(new RuntimeException("Failed to send record"));

        waitForCondition(producer::closed);
        assertTrue(producer.closed());

        List<ProducerRecord<HendelseKey, Hendelse>> history = producer.history();
        List<ProducerRecord<HendelseKey, Hendelse>> expected = Arrays.asList(
                new ProducerRecord<>(topic, HendelseKey.newBuilder()
                        .setGjelderPeriode("2018")
                        .setIdentifikator("123456789")
                        .build(), hendelseList.get(0)),
                new ProducerRecord<>(topic, HendelseKey.newBuilder()
                        .setGjelderPeriode("2018")
                        .setIdentifikator("234567890")
                        .build(), hendelseList.get(1)),
                new ProducerRecord<>(topic, HendelseKey.newBuilder()
                        .setGjelderPeriode("2018")
                        .setIdentifikator("345678901")
                        .build(), hendelseList.get(2)),
                new ProducerRecord<>(topic, HendelseKey.newBuilder()
                        .setGjelderPeriode("2018")
                        .setIdentifikator("456789012")
                        .build(), hendelseList.get(3)),
                new ProducerRecord<>(topic, HendelseKey.newBuilder()
                        .setGjelderPeriode("2018")
                        .setIdentifikator("567890123")
                        .build(), hendelseList.get(4))
        );

        assertEquals(expected, history);

        assertEquals(2, writer.lastWrittenSekvensnummer);
    }

    @Test
    void that_ProducerIsShutdown_When_SekvensnummerIsNotSentOk() {
        List<Hendelse> hendelseList = Arrays.asList(
                new Hendelse(1L, "123456789", "2018"),
                new Hendelse(2L, "234567890", "2018"),
                new Hendelse(3L, "345678901", "2018"),
                new Hendelse(4L, "456789012", "2018"),
                new Hendelse(5L, "567890123", "2018")
        );


        RuntimeException exception = new RuntimeException("Failed to write sekvensnummer");
        ExceptionThrowerSekvensnummerWriter evilWriter = new ExceptionThrowerSekvensnummerWriter(exception);
        SkatteoppgjorhendelseProducer skatteoppgjorhendelseProducer = new SkatteoppgjorhendelseProducer(producer, topic, evilWriter, EARLIEST_VALID_HENDELSE_YEAR);

        assertEquals((long)hendelseList.get(hendelseList.size() - 1).getSekvensnummer(), skatteoppgjorhendelseProducer.sendHendelser(hendelseList));

        producer.flush();
        waitForCondition(producer::closed);
        assertTrue(producer.closed());

        List<ProducerRecord<HendelseKey, Hendelse>> history = producer.history();
        List<ProducerRecord<HendelseKey, Hendelse>> expected = Arrays.asList(
                new ProducerRecord<>(topic, HendelseKey.newBuilder()
                        .setGjelderPeriode("2018")
                        .setIdentifikator("123456789")
                        .build(), hendelseList.get(0)),
                new ProducerRecord<>(topic, HendelseKey.newBuilder()
                        .setGjelderPeriode("2018")
                        .setIdentifikator("234567890")
                        .build(), hendelseList.get(1)),
                new ProducerRecord<>(topic, HendelseKey.newBuilder()
                        .setGjelderPeriode("2018")
                        .setIdentifikator("345678901")
                        .build(), hendelseList.get(2)),
                new ProducerRecord<>(topic, HendelseKey.newBuilder()
                        .setGjelderPeriode("2018")
                        .setIdentifikator("456789012")
                        .build(), hendelseList.get(3)),
                new ProducerRecord<>(topic, HendelseKey.newBuilder()
                        .setGjelderPeriode("2018")
                        .setIdentifikator("567890123")
                        .build(), hendelseList.get(4))
        );

        assertEquals(expected, history);

        assertEquals(-1, writer.lastWrittenSekvensnummer);
    }

    @Test
    void close() {
        SkatteoppgjorhendelseProducer skatteoppgjorhendelseProducer = new SkatteoppgjorhendelseProducer(producer, topic, writer, EARLIEST_VALID_HENDELSE_YEAR);

        assertFalse(producer.closed());
        skatteoppgjorhendelseProducer.shutdown();
        assertTrue(producer.closed());
    }

    private class DummySekvensnummerWriter implements SekvensnummerWriter {
        long lastWrittenSekvensnummer = -1;

        public void writeSekvensnummer(long nextSekvensnummer) {
            this.lastWrittenSekvensnummer = nextSekvensnummer;
        }
    }

    private class ExceptionThrowerSekvensnummerWriter implements SekvensnummerWriter {
        private final RuntimeException exception;

        ExceptionThrowerSekvensnummerWriter(RuntimeException exception) {
            this.exception = exception;
        }

        public void writeSekvensnummer(long nextSekvensnummer) {
            throw this.exception;
        }
    }
}
