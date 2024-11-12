/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mssql

import io.airbyte.cdk.command.CliRunner
import io.airbyte.cdk.command.SyncsTestFixture
import io.airbyte.cdk.output.BufferingOutputConsumer
import io.airbyte.cdk.util.Jsons
import io.airbyte.protocol.models.Field
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.*
import org.apache.commons.lang3.builder.ToStringBuilder
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test


class MsSqlServerSpecIntegrationTest {
    @Test
    fun testSpec() {
        SyncsTestFixture.testSpec("expected_spec.json")
    }

    @Test
    fun testCheck() {
        val it = MsSqlServerContainerFactory.shared(MsSqlServerContainerFactory.SQLSERVER_2022)
        SyncsTestFixture.testCheck(MsSqlServerContainerFactory.config(it))
    }

    @Test
    fun testDiscover() {
        val container = MsSqlServerContainerFactory.shared(MsSqlServerContainerFactory.SQLSERVER_2022)
        val config = MsSqlServerContainerFactory.config(container)
        val discoverOutput: BufferingOutputConsumer = CliRunner.source("discover", config).run()
        Assertions.assertEquals(listOf(AirbyteCatalog().withStreams(listOf(
            AirbyteStream()
                .withName("id_name_and_born")
                .withJsonSchema(Jsons.readTree("""{"type":"object","properties":{"born":{"type":"string"},"name":{"type":"string"},"id":{"type":"number","airbyte_type":"integer"}}}"""))
                .withSupportedSyncModes(listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
                .withSourceDefinedCursor(false)
                .withNamespace(config.schemas!![0])
                .withSourceDefinedPrimaryKey(listOf(listOf("id")))
                .withIsResumable(true),
            AirbyteStream()
                .withName("name_and_born")
                .withJsonSchema(Jsons.readTree("""{"type":"object","properties":{"born":{"type":"string"},"name":{"type":"string"}}}"""))
                .withSupportedSyncModes(listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
                .withSourceDefinedCursor(false)
                .withNamespace(config.schemas!![0])
        ))), discoverOutput.catalogs())
    }

    @Test
    fun testSync() {
        val container = MsSqlServerContainerFactory.shared(MsSqlServerContainerFactory.SQLSERVER_2022)
        val config = MsSqlServerContainerFactory.config(container)
        val configuredCatalog = ConfiguredAirbyteCatalog().withStreams(
            listOf(
                ConfiguredAirbyteStream()
                    .withSyncMode(SyncMode.INCREMENTAL)
                    .withCursorField(listOf("id"))
                    .withDestinationSyncMode(DestinationSyncMode.APPEND)
                    .withStream(
                        CatalogHelpers.createAirbyteStream(
                            "", "SCHEMA_NAME",
                            Field.of("id", JsonSchemaType.NUMBER),
                            Field.of("name", JsonSchemaType.STRING)
                        )
                            .withSupportedSyncModes(listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
                    ),
                ConfiguredAirbyteStream()
                    .withSyncMode(SyncMode.INCREMENTAL)
                    .withCursorField(listOf("id"))
                    .withDestinationSyncMode(DestinationSyncMode.APPEND)
                    .withStream(
                        CatalogHelpers.createAirbyteStream(
                            "STREAM_NAME2", "SCHEMA_NAME",
                            Field.of("id", JsonSchemaType.NUMBER),
                            Field.of("name", JsonSchemaType.STRING)
                        )
                            .withSupportedSyncModes(listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
                    )
            )
        )

        val readOutput: BufferingOutputConsumer =
            CliRunner.source("read", config, configuredCatalog, listOf()).run()
        println("SGX readOutput=${ToStringBuilder.reflectionToString(readOutput)}")
    }
}
