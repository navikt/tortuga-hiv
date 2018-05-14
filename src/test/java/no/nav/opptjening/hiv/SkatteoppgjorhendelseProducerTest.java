package no.nav.opptjening.hiv;

import no.nav.opptjening.hiv.sekvensnummer.SekvensnummerWriter;
import no.nav.opptjening.skatt.schema.hendelsesliste.Hendelse;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;

public class SkatteoppgjorhendelseProducerTest {
    private MockProducer<String, Hendelse> producer;
    private DummySekvensnummerWriter writer;

    private final String topic = "my-test-topic";

    @Before
    public void setUp() {
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
    public void that_SekvensnummerIsWritten_When_RecordsAreSentOk() {
        List<Hendelse> hendelseList = Arrays.asList(
                Hendelse.newBuilder().setSekvensnummer(1).setIdentifikator("123456789")
                        .setGjelderPeriode("2018").build(),
                Hendelse.newBuilder().setSekvensnummer(2).setIdentifikator("234567890")
                        .setGjelderPeriode("2018").build(),
                Hendelse.newBuilder().setSekvensnummer(3).setIdentifikator("345678901")
                        .setGjelderPeriode("2018").build(),
                Hendelse.newBuilder().setSekvensnummer(4).setIdentifikator("456789012")
                        .setGjelderPeriode("2018").build(),
                Hendelse.newBuilder().setSekvensnummer(5).setIdentifikator("567890123")
                        .setGjelderPeriode("2018").build()
        );

        SkatteoppgjorhendelseProducer skatteoppgjorhendelseProducer = new SkatteoppgjorhendelseProducer(producer, topic, writer);
        Assert.assertEquals((long)hendelseList.get(hendelseList.size() - 1).getSekvensnummer(), skatteoppgjorhendelseProducer.sendHendelser(hendelseList));

        producer.flush();

        List<ProducerRecord<String, Hendelse>> history = producer.history();
        List<ProducerRecord<String, Hendelse>> expected = Arrays.asList(
                new ProducerRecord<>(topic, "2018-123456789", hendelseList.get(0)),
                new ProducerRecord<>(topic, "2018-234567890", hendelseList.get(1)),
                new ProducerRecord<>(topic, "2018-345678901", hendelseList.get(2)),
                new ProducerRecord<>(topic, "2018-456789012", hendelseList.get(3)),
                new ProducerRecord<>(topic, "2018-567890123", hendelseList.get(4))
        );

        assertEquals(expected, history);

        Assert.assertEquals(hendelseList.get(hendelseList.size() - 1).getSekvensnummer() + 1, writer.lastWrittenSekvensnummer);
    }

    @Test
    public void that_SekvensnummerIsNotWritten_When_RecordsAreNotSentOk() {
        List<Hendelse> hendelseList = Arrays.asList(
                Hendelse.newBuilder().setSekvensnummer(1).setIdentifikator("123456789")
                        .setGjelderPeriode("2018").build(),
                Hendelse.newBuilder().setSekvensnummer(2).setIdentifikator("234567890")
                        .setGjelderPeriode("2018").build(),
                Hendelse.newBuilder().setSekvensnummer(3).setIdentifikator("345678901")
                        .setGjelderPeriode("2018").build(),
                Hendelse.newBuilder().setSekvensnummer(4).setIdentifikator("456789012")
                        .setGjelderPeriode("2018").build(),
                Hendelse.newBuilder().setSekvensnummer(5).setIdentifikator("567890123")
                        .setGjelderPeriode("2018").build()
        );

        SkatteoppgjorhendelseProducer skatteoppgjorhendelseProducer = new SkatteoppgjorhendelseProducer(producer, topic, writer);
        Assert.assertEquals((long)hendelseList.get(hendelseList.size() - 1).getSekvensnummer(), skatteoppgjorhendelseProducer.sendHendelser(hendelseList));

        producer.completeNext();
        producer.errorNext(new RuntimeException("Failed to send record"));

        waitForCondition(producer::closed);
        Assert.assertTrue(producer.closed());

        List<ProducerRecord<String, Hendelse>> history = producer.history();
        List<ProducerRecord<String, Hendelse>> expected = Arrays.asList(
                new ProducerRecord<>(topic, "2018-123456789", hendelseList.get(0)),
                new ProducerRecord<>(topic, "2018-234567890", hendelseList.get(1)),
                new ProducerRecord<>(topic, "2018-345678901", hendelseList.get(2)),
                new ProducerRecord<>(topic, "2018-456789012", hendelseList.get(3)),
                new ProducerRecord<>(topic, "2018-567890123", hendelseList.get(4))
        );

        assertEquals(expected, history);

        Assert.assertEquals(2, writer.lastWrittenSekvensnummer);
    }

    @Test
    public void that_ProducerIsShutdown_When_SekvensnummerIsNotSentOk() {
        List<Hendelse> hendelseList = Arrays.asList(
                Hendelse.newBuilder().setSekvensnummer(1).setIdentifikator("123456789")
                        .setGjelderPeriode("2018").build(),
                Hendelse.newBuilder().setSekvensnummer(2).setIdentifikator("234567890")
                        .setGjelderPeriode("2018").build(),
                Hendelse.newBuilder().setSekvensnummer(3).setIdentifikator("345678901")
                        .setGjelderPeriode("2018").build(),
                Hendelse.newBuilder().setSekvensnummer(4).setIdentifikator("456789012")
                        .setGjelderPeriode("2018").build(),
                Hendelse.newBuilder().setSekvensnummer(5).setIdentifikator("567890123")
                        .setGjelderPeriode("2018").build()
        );


        RuntimeException exception = new RuntimeException("Failed to write sekvensnummer");
        ExceptionThrowerSekvensnummerWriter evilWriter = new ExceptionThrowerSekvensnummerWriter(exception);
        SkatteoppgjorhendelseProducer skatteoppgjorhendelseProducer = new SkatteoppgjorhendelseProducer(producer, topic, evilWriter);

        Assert.assertEquals((long)hendelseList.get(hendelseList.size() - 1).getSekvensnummer(), skatteoppgjorhendelseProducer.sendHendelser(hendelseList));

        producer.flush();
        waitForCondition(producer::closed);
        Assert.assertTrue(producer.closed());

        List<ProducerRecord<String, Hendelse>> history = producer.history();
        List<ProducerRecord<String, Hendelse>> expected = Arrays.asList(
                new ProducerRecord<>(topic, "2018-123456789", hendelseList.get(0)),
                new ProducerRecord<>(topic, "2018-234567890", hendelseList.get(1)),
                new ProducerRecord<>(topic, "2018-345678901", hendelseList.get(2)),
                new ProducerRecord<>(topic, "2018-456789012", hendelseList.get(3)),
                new ProducerRecord<>(topic, "2018-567890123", hendelseList.get(4))
        );

        assertEquals(expected, history);

        Assert.assertEquals(-1, writer.lastWrittenSekvensnummer);
    }

    @Test
    public void close() {
        SkatteoppgjorhendelseProducer skatteoppgjorhendelseProducer = new SkatteoppgjorhendelseProducer(producer, topic, writer);

        Assert.assertFalse(producer.closed());
        skatteoppgjorhendelseProducer.shutdown();
        Assert.assertTrue(producer.closed());
    }

    private class DummySekvensnummerWriter implements SekvensnummerWriter {
        public long lastWrittenSekvensnummer = -1;

        public void writeSekvensnummer(long nextSekvensnummer) {
            this.lastWrittenSekvensnummer = nextSekvensnummer;
        }
    }

    private class ExceptionThrowerSekvensnummerWriter implements SekvensnummerWriter {
        private final RuntimeException exception;

        public ExceptionThrowerSekvensnummerWriter(RuntimeException exception) {
            this.exception = exception;
        }

        public void writeSekvensnummer(long nextSekvensnummer) {
            throw this.exception;
        }
    }
}
