package com.airwallex.kafka.samples;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

class ConsumerConfigs {

    private static final Logger logger = LogManager.getLogger(ConsumerConfigs.class);

    static Properties consumerConfig() {

        Properties consumerProps = new Properties();
        Properties loadProps = new Properties();
        consumerProps.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (FileInputStream file = new FileInputStream(MM2GroupOffsetSync.propertiesFilePath)) {
            loadProps.load(file);
            consumerProps.setProperty("bootstrap.servers", loadProps.getProperty("bootstrap.servers"));
            consumerProps.setProperty("src.bootstrap.servers", loadProps.getProperty("src.bootstrap.servers"));
            consumerProps.setProperty("security.protocol", loadProps.getProperty("security.protocol"));
            consumerProps.setProperty("ssl.truststore.location", loadProps.getProperty("ssl.truststore.location"));
            consumerProps.setProperty("ssl.keystore.location", loadProps.getProperty("ssl.keystore.location"));
            consumerProps.setProperty("ssl.truststore.password", loadProps.getProperty("ssl.truststore.password"));
            consumerProps.setProperty("ssl.keystore.password", loadProps.getProperty("ssl.keystore.password"));
            consumerProps.setProperty("sasl.mechanism", loadProps.getProperty("sasl.mechanism"));
            consumerProps.setProperty("sasl.jaas.config", loadProps.getProperty("sasl.jaas.config"));

        } catch (IOException e) {
            logger.info("Properties file not found in location: {}, using defaults \n", MM2GroupOffsetSync.propertiesFilePath);
        }

        return consumerProps;
    }


}
