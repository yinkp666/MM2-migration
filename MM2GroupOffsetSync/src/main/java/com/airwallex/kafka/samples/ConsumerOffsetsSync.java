package com.airwallex.kafka.samples;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.mirror.RemoteClusterUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.*;

class ConsumerOffsetsSync implements Runnable {
    private final AdminClient adminClient;
    private final AdminClient srcAdminClient;
    private static final Logger logger = LogManager.getLogger(ConsumerOffsetsSync.class);

    ConsumerOffsetsSync(AdminClient adminClient, AdminClient srcAdminClient) {
        this.adminClient = adminClient;
        this.srcAdminClient = srcAdminClient;
    }

    private Map<TopicPartition, OffsetAndMetadata> getCheckpointOffsets(String consumerGroupId) {
        Map<String, Object> mm2config = new MM2Config().mm2config();
        Map<TopicPartition, OffsetAndMetadata> translatedOffsets = new HashMap<>();
        try {
            translatedOffsets.putAll(RemoteClusterUtils.translateOffsets(mm2config, (String) mm2config.get("source.cluster.alias"), consumerGroupId, Duration.ofSeconds(20L)));
            if (translatedOffsets.size() == 0) {
                logger.info("Could not find or get translated offsets for consumer group id: {} \n", consumerGroupId);
            } else {
                logger.info("{} - Translated offsets for consumer group from checkpoint {} - {} \n", System.currentTimeMillis(), consumerGroupId, translatedOffsets);
            }
        } catch (InterruptedException | TimeoutException e) {
            logger.info("Error retrieving translated offsets. Returning empty map. \n");
            logger.error(Util.stackTrace(e));
        }
        return translatedOffsets;
    }

    private boolean checkConsumerGroups(String consumerGroupId) {
        if (adminClient != null) {
            DescribeConsumerGroupsResult describeConsumerGroupsResult = adminClient.describeConsumerGroups(Collections.singletonList(consumerGroupId));
            Map<String, ConsumerGroupDescription> consumerGroupDescriptions;
            try {
                consumerGroupDescriptions = describeConsumerGroupsResult.all().get();
                logger.info("ConsumerGroupDescriptions for consumer group {} - {} \n", consumerGroupId, consumerGroupDescriptions);
                return consumerGroupDescriptions.get(consumerGroupId).state() == ConsumerGroupState.EMPTY || (consumerGroupDescriptions.get(consumerGroupId).state() == ConsumerGroupState.DEAD);
            } catch (InterruptedException | ExecutionException e) {
                logger.error(Util.stackTrace(e));
                return false;
            }

        } else {
            logger.error("AdminClient is null. Cannot proceed. \n");
            return false;
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> listConsumerOffsets(String consumerGroupId, Map<TopicPartition, OffsetAndMetadata> consumerOffsets) {
        Map<TopicPartition, OffsetAndMetadata> filteredConsumerGroupOffsets;
        if (adminClient != null) {
            try {
                final Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets = new HashMap<>(adminClient.listConsumerGroupOffsets(consumerGroupId).partitionsToOffsetAndMetadata().get());
                logger.info("Consumer Group Offsets at destination for consumer group id {} - {} \n", consumerGroupId, consumerGroupOffsets);

                if (consumerGroupOffsets.size() > 0) {
                    logger.info("Checking to see if the translated offsets are higher than destination consumer offsets \n");
                    filteredConsumerGroupOffsets = consumerOffsets.entrySet().stream().filter(i -> i.getValue().offset() > consumerGroupOffsets.get(new TopicPartition(i.getKey().topic(), i.getKey().partition())).offset()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    if (filteredConsumerGroupOffsets.size() > 0) {
                        logger.info("Filtered consumer offsets - {} \n", filteredConsumerGroupOffsets);
                    } else {
                        logger.info("Translated offsets for consumer group id {} either smaller or caught up \n", consumerGroupId);
                    }
                } else {
                    logger.info("No Consumer Group offsets for specified consumer group id {}. Returning translated consumer offsets. \n", consumerGroupId);
                    filteredConsumerGroupOffsets = consumerOffsets;
                }

            } catch (InterruptedException | ExecutionException e) {
                logger.info("Error retrieving Consumer Group offsets. Returning Empty Map. \n");
                filteredConsumerGroupOffsets = Collections.emptyMap();
                logger.error(Util.stackTrace(e));
            }
        } else {
            filteredConsumerGroupOffsets = Collections.emptyMap();
            logger.error("AdminClient is null. Cannot proceed. Returning Empty Map. \n");
        }
        return filteredConsumerGroupOffsets;
    }

    private void updateConsumerGroupOffsets(String consumerGroupId, Map<TopicPartition, OffsetAndMetadata> consumerOffsets) {

        if (adminClient != null) {
            Map<TopicPartition, OffsetAndMetadata> consumergroupOffsets = listConsumerOffsets(consumerGroupId, consumerOffsets);
            if (consumergroupOffsets.size() > 0) {
                AlterConsumerGroupOffsetsResult alterConsumerGroupOffsetsResult = adminClient.alterConsumerGroupOffsets(consumerGroupId, consumergroupOffsets);
                try {
                    alterConsumerGroupOffsetsResult.all().get();
                    logger.info("Updated offsets for consumer group: {} with offsets {} \n", consumerGroupId, consumerOffsets);
                } catch (InterruptedException | ExecutionException e) {
                    logger.error(Util.stackTrace(e));
                }

            } else {
                logger.info("No translated offsets received for consumer group id {}. Nothing to update. \n", consumerGroupId);
            }
        } else {
            logger.error("AdminClient is null. Cannot proceed. \n");
        }
    }

    @Override
    public void run() {
        ListConsumerGroupsResult listConsumerGroupsResult = srcAdminClient.listConsumerGroups();
        Collection<ConsumerGroupListing> consumerGroupListings = null;
        try {
            consumerGroupListings = listConsumerGroupsResult.valid().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        for(ConsumerGroupListing cg : consumerGroupListings) {
            String consumerGroupId = cg.groupId();
            logger.info("--------------->" + consumerGroupId);
            if(!consumerGroupId.startsWith("oprtr")){
            if (checkConsumerGroups(consumerGroupId)) {
                updateConsumerGroupOffsets(consumerGroupId, getCheckpointOffsets(consumerGroupId));
            }
            }
        }
//        if (checkConsumerGroups(consumerGroupId)) {
//            updateConsumerGroupOffsets(consumerGroupId, getCheckpointOffsets(consumerGroupId));
//        }

    }
}
