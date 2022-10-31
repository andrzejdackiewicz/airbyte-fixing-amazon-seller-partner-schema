/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.integrations.base.Destination;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.destination.iceberg.config.IcebergCatalogConfig;
import io.airbyte.integrations.destination.iceberg.config.IcebergCatalogConfigFactory;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteConnectionStatus.Status;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

@Slf4j
public class IcebergDestination extends BaseConnector implements Destination {

    private final IcebergCatalogConfigFactory icebergCatalogConfigFactory;

    public IcebergDestination() {
        this.icebergCatalogConfigFactory = new IcebergCatalogConfigFactory();
    }

    @VisibleForTesting
    public IcebergDestination(IcebergCatalogConfigFactory icebergCatalogConfigFactory) {
        this.icebergCatalogConfigFactory = Objects.requireNonNullElseGet(icebergCatalogConfigFactory,
            IcebergCatalogConfigFactory::new);
    }

    public static void main(String[] args) throws Exception {
        new IntegrationRunner(new IcebergDestination()).run(args);
    }

    @Override
    public AirbyteConnectionStatus check(JsonNode config) {
        try {
            IcebergCatalogConfig icebergCatalogConfig = icebergCatalogConfigFactory.fromJsonNodeConfig(config);
            icebergCatalogConfig.check();

            //getting here means s3 check success
            return new AirbyteConnectionStatus().withStatus(Status.SUCCEEDED);
        } catch (final Exception e) {
            log.error("Exception attempting to access the S3 bucket: ", e);
            return new AirbyteConnectionStatus()
                .withStatus(AirbyteConnectionStatus.Status.FAILED)
                .withMessage("Could not connect to the S3 bucket with the provided configuration. \n" + e
                    .getMessage());
        }
    }

    @Override
    public AirbyteMessageConsumer getConsumer(JsonNode config,
        ConfiguredAirbyteCatalog catalog,
        Consumer<AirbyteMessage> outputRecordCollector) {
        final IcebergCatalogConfig icebergCatalogConfig = this.icebergCatalogConfigFactory.fromJsonNodeConfig(config);
        Map<String, String> sparkConfMap = icebergCatalogConfig.sparkConfigMap();

        log.debug("icebergCatalogConfig:{}, sparkConfMap:{}", icebergCatalogConfig, sparkConfMap);

        Builder sparkBuilder = SparkSession.builder()
            .master("local")
            .appName("Airbyte->Iceberg-" + System.currentTimeMillis());
        sparkConfMap.forEach(sparkBuilder::config);
        SparkSession spark = sparkBuilder.getOrCreate();

        return new IcebergConsumer(spark, outputRecordCollector, catalog, icebergCatalogConfig);
    }

}
