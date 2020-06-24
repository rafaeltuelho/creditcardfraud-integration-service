package com.redhat.demo.dm.ccfraud;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import com.redhat.demo.dm.ccfraud.domain.CreditCardTransaction;
import com.redhat.demo.dm.ccfraud.domain.PotentialFraudFact;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.kie.api.KieServices;
import org.kie.api.builder.KieScanner;
import org.kie.api.builder.ReleaseId;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.EntryPoint;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.time.SessionClock;
import org.kie.api.time.SessionPseudoClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private static final DateTimeFormatter DATE_TIME_FORMAT = 
        DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss:SSS", Locale.US);

    private static Vertx vertx = Vertx.vertx();
    private static KieContainer kContainer;
    private static CreditCardTransactionRepository cctRepository = new InMemoryCreditCardTransactionRepository();

    public static void main(String args[]) {
		// Load the Drools KIE-Container.
        KieServices kieServices = KieServices.Factory.get();
        kContainer = kieServices.getKieClasspathContainer();
        
        Main creditCardFraudVerticle = new Main();
        creditCardFraudVerticle.exampleCreateConsumerJava(vertx);
    }

    public void exampleCreateConsumerJava(Vertx vertx) {
        // creating the consumer using properties config
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
            System.getProperty("kafka.cluster.url", "my-cluster-kafka-brokers:9092"));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, System.getProperty("kafka.cluster.group.id", "test"));

        // use consumer for interacting with Apache Kafka
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);

        // subscribe to several topics
        Set<String> topics = new HashSet<>();
        topics.add("events");
        consumer.subscribe(topics);

        // or just subscribe to a single topic
        consumer.subscribe(topics, ar -> {
            if (ar.succeeded()) {
                LOGGER.info("Subscribed to Kafka topic");
            } else {
                LOGGER.warn("Could not subscribe to kafka " + ar.cause().getMessage());
            }
        });

        consumer.handler(record -> {
            LOGGER.info("New event received!");
            LOGGER.info(new Gson().fromJson(record.value(), CreditCardTransaction.class).toString());

            CreditCardTransaction creditCardTransaction = new Gson().fromJson(record.value(),
                    CreditCardTransaction.class);
            processTransaction(creditCardTransaction);
        });
    }

    private static void processTransaction(CreditCardTransaction ccTransaction) {
        // Retrieve all transactions for this account
        Collection<CreditCardTransaction> ccTransactions = cctRepository
                .getCreditCardTransactionsForCC(ccTransaction.getCreditCardNumber());

        if (ccTransactions == null) {
            LOGGER.info("no previous transactions found for Credit Card {}", ccTransaction.getCreditCardNumber());
            return;
        }

        LOGGER.info("Found '" + ccTransactions.size() + 
            "' transactions for creditcard: '" + ccTransaction.getCreditCardNumber() + "'.");

        //will get automatically upgraded by the KieScanner if new version is found in the Maven Repo
        KieSession kieSession = kContainer.newKieSession();
        LOGGER.info("Get the kie session [ {} ]", kieSession.getIdentifier());

        // Insert transaction history/context.
        LOGGER.info("Inserting previous (recent) credit card transactions into session.");
        for (CreditCardTransaction nextTransaction : ccTransactions) {
            insert(kieSession, "Transactions", nextTransaction);
        }

        // Insert the new transaction event
        LOGGER.info(" ");
        LOGGER.info("Inserting credit card transaction event into session.");
        insert(kieSession, "Transactions", ccTransaction);

        // And fire the rules.
        LOGGER.info("Firing the rules with {} Facts in the Session [ {} ] WM", 
            kieSession.getFactCount(), kieSession.getIdentifier());
        int fired = kieSession.fireAllRules();
        LOGGER.info("{} rules got fired!", fired);

        Collection<?> fraudResponse = kieSession.getObjects();

        for (Object object : fraudResponse) {
            String jsonString = new Gson().toJson(object);
            PotentialFraudFact potentialFraudFact = new Gson().fromJson(jsonString, PotentialFraudFact.class);
            LOGGER.info("PotentialFraudFact" + potentialFraudFact);

            Main.invokeCase(potentialFraudFact);
        }

        // Dispose the session to free up the resources.
        kieSession.dispose();
    }

	/**
	 * CEP insert method that inserts the event into the Drools CEP session and programatically advances the session clock to the time of
	 * the current event.
	 * 
	 * @param kieSession
	 *            the session in which to insert the event.
	 * @param stream
	 *            the name of the Drools entry-point in which to insert the event.
	 * @param cct
	 *            the event to insert.
	 * 
	 * @return the {@link FactHandle} of the inserted fact.
	 */
	private static FactHandle insert(KieSession kieSession, String stream, CreditCardTransaction cct) {
		SessionClock clock = kieSession.getSessionClock();
		if (!(clock instanceof SessionPseudoClock)) {
			String errorMessage = "This fact inserter can only be used with KieSessions that use a SessionPseudoClock";
			LOGGER.error(errorMessage);
			throw new IllegalStateException(errorMessage);
        }
        
		SessionPseudoClock pseudoClock = (SessionPseudoClock) clock;
        LOGGER.info( "\tCEP Engine PseudoClock current time: " + 
            LocalDateTime.ofInstant(Instant.ofEpochMilli(pseudoClock.getCurrentTime()), ZoneId.systemDefault()).toString() );
		EntryPoint ep = kieSession.getEntryPoint(stream);

		// First insert the event
		FactHandle factHandle = ep.insert(cct);
		// And then advance the clock.
		LOGGER.info(" ");
        LOGGER.info("Inserting credit card [" + cct.getCreditCardNumber() + "] transaction [" + 
            cct.getTransactionNumber() + "] context into session.");
		String dateTimeFormatted = LocalDateTime.ofInstant(
			Instant.ofEpochMilli(cct.getTimestamp()), ZoneId.systemDefault()).format(DATE_TIME_FORMAT);
		LOGGER.info( "\tCC Transaction Time: " + dateTimeFormatted);
        long advanceTime = cct.getTimestamp() - pseudoClock.getCurrentTime();
        
		if (advanceTime > 0) {
			long tSec = advanceTime/1000;
			LOGGER.info("\tAdvancing the PseudoClock with " + advanceTime + " milliseconds (" + tSec + "sec)" );
			
			pseudoClock.advanceTime(advanceTime, TimeUnit.MILLISECONDS);
			dateTimeFormatted = LocalDateTime.ofInstant(
				Instant.ofEpochMilli(pseudoClock.getCurrentTime()), ZoneId.systemDefault()).format(DATE_TIME_FORMAT);
			LOGGER.info( "\tCEP Engine PseudoClock ajusted time: " +  dateTimeFormatted);
		} else {
			// Print a warning when we don't need to advance the clock. This usually means that the events are entering the system in the
			// incorrect order.
            LOGGER.warn("Not advancing time. CreditCardTransaction timestamp is '" +
             cct.getTimestamp() + "', PseudoClock timestamp is '" + pseudoClock.getCurrentTime() + "'.");
        }
        
		return factHandle;
	}

    private static void invokeCase(PotentialFraudFact potentialFraudFact) {
        vertx.<String>executeBlocking(future -> {
            try {
                CaseMgmt caseMgmt = new CaseMgmt();
                caseMgmt.invokeCase(potentialFraudFact);
            } catch (Exception e) {
                LOGGER.error("Business central not yet ready...");
            }
        }, res -> {
            if (res.succeeded()) {
                LOGGER.info(res.toString());
            }
        });
    }

}
