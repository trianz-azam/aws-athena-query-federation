/*-
 * #%L
 * athena-gcs
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.gcs;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.*;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.gcs.storage.datasource.StorageDatasourceFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.*;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.PageImpl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.CLASSIFICATION_GLUE_TABLE_PARAM;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.PARTITION_PATTERN_PATTERN;
import static com.amazonaws.athena.connectors.gcs.GcsTestUtils.createColumn;
import static com.amazonaws.athena.connectors.gcs.filter.FilterExpressionBuilderTest.createSummaryWithLValueRangeEqual;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*", "javax.security.*"})
@PrepareForTest({StorageOptions.class, StorageDatasourceFactory.class, GoogleCredentials.class, GcsSchemaUtils.class, AWSSecretsManagerClientBuilder.class, ServiceAccountCredentials.class, AWSGlueClientBuilder.class, GlueMetadataHandler.class})
public class GcsMetadataHandlerTest
{
    private static final String QUERY_ID = "queryId";
    private static final String CATALOG = "catalog";
    private static final String TEST_TOKEN = "testToken";
    private static final String SCHEMA_NAME = "default";
    private static final TableName TABLE_NAME = new TableName("default", "testtable");
    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();
    @Mock
    protected PageImpl<Blob> tables;
    @Mock
    GoogleCredentials credentials;
    @Mock
    AmazonS3 amazonS3;
    private GcsMetadataHandler gcsMetadataHandler;
    private BlockAllocator blockAllocator;
    private FederatedIdentity federatedIdentity;
    private QueryStatusChecker queryStatusChecker;
    @Mock
    private AWSGlue awsGlue;
    @Mock
    private AWSSecretsManager secretsManager;
    @Mock
    private ServiceAccountCredentials serviceAccountCredentials;
    @Mock
    private AmazonAthena athena;

    @Before
    public void setUp() throws Exception
    {
        Storage storage = mock(Storage.class);
        Blob blob = mock(Blob.class);
        Blob blob1 = mock(Blob.class);
        mockStatic(StorageOptions.class);
        StorageOptions.Builder optionBuilder = mock(StorageOptions.Builder.class);
        PowerMockito.when(StorageOptions.newBuilder()).thenReturn(optionBuilder);
        StorageOptions mockedOptions = mock(StorageOptions.class);
        PowerMockito.when(optionBuilder.setCredentials(ArgumentMatchers.any())).thenReturn(optionBuilder);
        PowerMockito.when(optionBuilder.build()).thenReturn(mockedOptions);
        PowerMockito.when(mockedOptions.getService()).thenReturn(storage);
        PowerMockito.when(storage.list(anyString(), Mockito.any())).thenReturn(tables);
        PowerMockito.when(tables.iterateAll()).thenReturn(List.of(blob, blob1));
        PowerMockito.when(blob.getName()).thenReturn("data.parquet");
        PowerMockito.when(blob1.getName()).thenReturn("birthday/year=2000/birth_month09/12/");
        environmentVariables.set("gcs_credential_key", "gcs_credential_keys");
        mockStatic(ServiceAccountCredentials.class);
        PowerMockito.when(ServiceAccountCredentials.fromStream(Mockito.any())).thenReturn(serviceAccountCredentials);
        MockitoAnnotations.initMocks(this);
        mockStatic(GoogleCredentials.class);
        PowerMockito.when(GoogleCredentials.fromStream(Mockito.any())).thenReturn(credentials);
        PowerMockito.when(credentials.createScoped((Collection<String>) any())).thenReturn(credentials);

        mockStatic(AWSSecretsManagerClientBuilder.class);
        PowerMockito.when(AWSSecretsManagerClientBuilder.defaultClient()).thenReturn(secretsManager);
        GetSecretValueResult getSecretValueResult = new GetSecretValueResult().withVersionStages(List.of("v1")).withSecretString("{\"gcs_credential_keys\": \"test\"}");
        Mockito.when(secretsManager.getSecretValue(Mockito.any())).thenReturn(getSecretValueResult);
        mockStatic(AWSGlueClientBuilder.class);
        PowerMockito.when(AWSGlueClientBuilder.defaultClient()).thenReturn(awsGlue);
        gcsMetadataHandler = new GcsMetadataHandler(new LocalKeyFactory(), secretsManager, athena, "spillBucket", "spillPrefix", amazonS3, awsGlue);
        blockAllocator = new BlockAllocatorImpl();
        federatedIdentity = Mockito.mock(FederatedIdentity.class);
    }

    @Test
    public void testDoListSchemaNames() throws Exception
    {
        GetDatabasesResult result = new GetDatabasesResult().withDatabaseList(
                new Database().withName("gcsdatabase").withLocationUri("s3://gcs"),
                new Database().withName("s3database").withLocationUri("s3://gcs"));
        ListSchemasRequest listSchemasRequest = new ListSchemasRequest(federatedIdentity,
                QUERY_ID, CATALOG);
        PowerMockito.when(awsGlue.getDatabases(any())).thenReturn(result);
        ListSchemasResponse schemaNamesResponse = gcsMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);
        List<String> expectedSchemaNames = new ArrayList<>();
        expectedSchemaNames.add("gcsdatabase");
        expectedSchemaNames.add("s3database");
        assertEquals(expectedSchemaNames, new ArrayList<>(schemaNamesResponse.getSchemas()));
    }

    @Test(expected = RuntimeException.class)
    public void testDoListSchemaNamesThrowsException() throws Exception
    {
        ListSchemasRequest listSchemasRequest = mock(ListSchemasRequest.class);
        when(listSchemasRequest.getCatalogName()).thenThrow(new RuntimeException("RuntimeException() "));
        ListSchemasResponse listSchemasResponse = gcsMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);
        assertNull(listSchemasResponse);
    }

    @Test
    public void testDoListTables() throws Exception
    {
        GetTablesResult getTablesResult = new GetTablesResult();
        List<Table> tableList = new ArrayList<>();
        tableList.add(new Table().withName("testtable1")
                .withParameters(ImmutableMap.of("classification", "parquet"))
                .withStorageDescriptor(new StorageDescriptor()
                        .withLocation("gs://default/testtable1/")));
        tableList.add(new Table().withName("testtable2")
                .withParameters(ImmutableMap.of())
                .withStorageDescriptor(new StorageDescriptor()
                        .withLocation("gs://default/testtable2/")
                        .withParameters(ImmutableMap.of("classification", "parquet"))));
        getTablesResult.setTableList(tableList);
        PowerMockito.when(awsGlue.getTables(any())).thenReturn(getTablesResult);
        ListTablesRequest listTablesRequest = new ListTablesRequest(federatedIdentity, QUERY_ID, CATALOG, SCHEMA_NAME, TEST_TOKEN, 50);
        ListTablesResponse tableNamesResponse = gcsMetadataHandler.doListTables(blockAllocator, listTablesRequest);
        assertEquals(2, tableNamesResponse.getTables().size());
    }

    @Test(expected = RuntimeException.class)
    public void testDoListTablesThrowsException() throws Exception
    {
        ListTablesRequest listTablesRequest = mock(ListTablesRequest.class);
        when(listTablesRequest.getCatalogName()).thenThrow(new RuntimeException("RunTimeException() "));
        ListTablesResponse listTablesResponse = gcsMetadataHandler.doListTables(blockAllocator, listTablesRequest);
        assertNull(listTablesResponse);
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        Field field = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Map<String, String> metadataSchema = new HashMap<>();
        metadataSchema.put("dataFormat", "parquet");
        Schema schema = new Schema(asList(field), metadataSchema);
        GetTableRequest getTableRequest = new GetTableRequest(federatedIdentity, QUERY_ID, "gcs", new TableName(SCHEMA_NAME, "testtable"));
        Context context =mock(Context.class);
        when(context.getInvokedFunctionArn()).thenReturn("arn:aws:lambda:us-east-1:12345678910:function:gcs-lambda");
        getTableRequest.setContext(context);
        Table table = new Table();
        table.setName("testtable");
        table.setDatabaseName("default");
        table.setParameters(ImmutableMap.of("classification", "parquet"));
        table.setStorageDescriptor(new StorageDescriptor()
                .withLocation("gs://default/testtable/").withColumns(new Column()));
        table.setCatalogId(CATALOG);
        List<Column> columns = List.of(
                createColumn("year", "bigint"),
                createColumn("month", "int"),
                createColumn("day", "int")
        );
        table.setPartitionKeys(columns);
        GetTableResult getTableResult = new GetTableResult();
        getTableResult.setTable(table);
        PowerMockito.when(awsGlue.getTable(any())).thenReturn(getTableResult);
        mockStatic(GcsSchemaUtils.class);
        PowerMockito.when(GcsSchemaUtils.buildTableSchema(any(), any())).thenReturn(schema);
        GetTableResponse res = gcsMetadataHandler.doGetTable(blockAllocator, getTableRequest);
        Field expectedField = res.getSchema().findField("name");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(expectedField.getType()));
    }

    @Test
    public void testGetPartitions() throws Exception
    {
        environmentVariables.set("glue_catalog", "12345678910");
        Field field = new Field("year", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Schema schema = SchemaBuilder.newBuilder().addField("id", new ArrowType.Int(64, false)).build();
        Table table = new Table();
        table.setName("birthday");
        table.setDatabaseName("mydatalake1");
        table.setParameters(ImmutableMap.of("classification", "parquet",
                "partition.pattern", "year={year}/birth_month{month}/{day}")
        );
        table.setStorageDescriptor(new StorageDescriptor()
                .withLocation("gs://mydatalake1test/birthday/").withColumns(new Column()));
        table.setCatalogId(CATALOG);
        List<Column> columns = List.of(
                createColumn("year", "bigint"),
                createColumn("month", "int"),
                createColumn("day", "int")
        );
        table.setPartitionKeys(columns);
        GetTableResult getTableResult = new GetTableResult();
        getTableResult.setTable(table);
        PowerMockito.when(awsGlue.getTable(any())).thenReturn(getTableResult);
        GetTableLayoutRequest getTableLayoutRequest = Mockito.mock(GetTableLayoutRequest.class);
        Mockito.when(getTableLayoutRequest.getTableName()).thenReturn(new TableName("mydatalake1", "birthday"));
        Mockito.when(getTableLayoutRequest.getCatalogName()).thenReturn("fakedatabase");
        Mockito.when(getTableLayoutRequest.getSchema()).thenReturn(schema);
        Constraints constraints = new Constraints(createSummaryWithLValueRangeEqual("id", new ArrowType.Int(64, false), 1L));
        Mockito.when(getTableLayoutRequest.getConstraints()).thenReturn(constraints);
        BlockWriter blockWriter = Mockito.mock(BlockWriter.class);
        gcsMetadataHandler.getPartitions(blockWriter, getTableLayoutRequest, queryStatusChecker);
    }

    @Test
    public void testDoGetSplits() throws Exception
    {
        Block partitions = BlockUtils.newBlock(blockAllocator, "year", Types.MinorType.INT.getType(), 2000);
        GetSplitsRequest request = new GetSplitsRequest(federatedIdentity,
                QUERY_ID, CATALOG, TABLE_NAME,
                partitions, List.of("year"), new Constraints(new HashMap<>()), null);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);
        GetTableResult getTableResult = mock(GetTableResult.class);
        StorageDescriptor storageDescriptor = mock(StorageDescriptor.class);
        when(storageDescriptor.getLocation()).thenReturn("gs://mydatalake1test/birthday/");
        Table table = mock(Table.class);
        when(table.getStorageDescriptor()).thenReturn(storageDescriptor);
        when(table.getParameters()).thenReturn(Map.of(PARTITION_PATTERN_PATTERN, "year={year}/", CLASSIFICATION_GLUE_TABLE_PARAM, "parquet"));
        when(awsGlue.getTable(any())).thenReturn(getTableResult);
        when(getTableResult.getTable()).thenReturn(table);
        List<Column> columns = List.of(
                createColumn("year", "bigint")
        );
        when(table.getPartitionKeys()).thenReturn(columns);
        GetSplitsResponse response = gcsMetadataHandler.doGetSplits(blockAllocator, request);
        assertNotNull(response);
    }
}
