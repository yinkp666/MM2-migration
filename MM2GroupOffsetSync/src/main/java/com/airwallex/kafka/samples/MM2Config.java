package com.airwallex.kafka.samples;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.connect.mirror.MirrorClientConfig;
import java.util.HashMap;
import java.util.Map;

class MM2Config {

    Map<String, Object> mm2config() {
        Map<String, Object> mm2Props = new HashMap<>();
        mm2Props.put("bootstrap.servers", ConsumerConfigs.consumerConfig().getProperty("bootstrap.servers"));
        mm2Props.put("source.cluster.alias", MM2GroupOffsetSync.sourceCluster);
        mm2Props.put("sasl.mechanism", ConsumerConfigs.consumerConfig().getProperty("sasl.mechanism"));
        mm2Props.put("sasl.jaas.config", ConsumerConfigs.consumerConfig().getProperty("sasl.jaas.config"));
        mm2Props.put("security.protocol",ConsumerConfigs.consumerConfig().getProperty("security.protocol"));
        mm2Props.put("ssl.truststore.location", ConsumerConfigs.consumerConfig().getProperty("ssl.truststore.location"));
        mm2Props.put("ssl.keystore.location", ConsumerConfigs.consumerConfig().getProperty("ssl.keystore.location"));
        mm2Props.put("ssl.truststore.password", ConsumerConfigs.consumerConfig().getProperty("ssl.truststore.password"));
        mm2Props.put("ssl.keystore.password", ConsumerConfigs.consumerConfig().getProperty("ssl.keystore.password"));
        if (!MM2GroupOffsetSync.replicationPolicyClass.equalsIgnoreCase(String.valueOf(MirrorClientConfig.REPLICATION_POLICY_CLASS_DEFAULT))){
            mm2Props.put(MirrorClientConfig.REPLICATION_POLICY_CLASS, MM2GroupOffsetSync.replicationPolicyClass);
        }

        return mm2Props;
    }
}