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
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class GcsMetadataHandler
        extends MetadataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GcsMetadataHandler.class);

    // TODO: hard-code single to table to test Dataset
    private static final String STORAGE_FILE = "s3://GOOG1EGNWCPMWNY5IOMRELOVM22ZQEBEVDS7NXL5GOSRX6BA2F7RMA6YJGO3Q:haK0skzuPrUljknEsfcRJCYRXklVAh+LuaIiirh1@athena-integ-test-1/bing_covid-19_data.parquet?endpoint_override=https%3A%2F%2Fstorage.googleapis.com";

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    private static final String SOURCE_TYPE = "gcs";
//    private static final GcsSchemaUtils gcsSchemaUtils = new GcsSchemaUtils();

    public GcsMetadataHandler()
    {
        super(SOURCE_TYPE);
    }

    @VisibleForTesting
    @SuppressWarnings("unused")
    protected GcsMetadataHandler(EncryptionKeyFactory keyFactory,
                                 AWSSecretsManager awsSecretsManager,
                                 AmazonAthena athena,
                                 String spillBucket,
                                 String spillPrefix,
                                 /** GcsSchemaUtils gcsSchemaUtils, */
                                 AmazonS3 amazonS3)
    {
        super(keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);
    }

    /**
     * Used to get the list of schemas (aka databases) that this source contains.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request   Provides details on who made the request and which Athena catalog they are querying.
     * @return A ListSchemasResponse which primarily contains a Set<String> of schema names and a catalog name
     * corresponding the Athena catalog that was queried.
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        LOGGER.debug("doListSchemaNames: {}", request.getCatalogName());
        List<String> schemas = List.of("default");
        return new ListSchemasResponse(request.getCatalogName(), schemas);
    }

    /**
     * Used to get the list of tables that this source contains.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request   Provides details on who made the request and which Athena catalog and database they are querying.
     * @return A ListTablesResponse which primarily contains a List<TableName> enumerating the tables in this
     * catalog, database tuple. It also contains the catalog name corresponding the Athena catalog that was queried.
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, final ListTablesRequest request) throws IOException
    {
//        LOGGER.debug("MetadataHandler=GcsMetadataHandler|Method=doListTables|Message=queryId {}",
//                request.getQueryId());
//        printJson(request, "doListTables");
//        List<TableName> tables = new ArrayList<>();
//        String nextToken;
//        LOGGER.info("MetadataHandler=GcsMetadataHandler|Method=doListTables|Message=Fetching list of tables with page size {} and token {} for scheme {}",
//                request.getPageSize(), request.getNextToken(), request.getSchemaName());
//        TableListResult result = datasource.getAllTables(request.getSchemaName(), request.getNextToken(),
//                request.getPageSize());
//        nextToken = result.getNextToken();
//        List<StorageObject> tableNames = result.getTables();
//        LOGGER.debug("MetadataHandler=GcsMetadataHandler|Method=doListTables|Message=tables under schema {} are: {}",
//                request.getSchemaName(), tableNames);
//        tableNames.forEach(storageObject -> tables.add(new TableName(request.getSchemaName(), storageObject.getTableName())));
        List<TableName> tables = new ArrayList<>();
        tables.add(new TableName(request.getSchemaName(), "ing_covid-19_data"));
        return new ListTablesResponse(request.getCatalogName(), tables, null);
    }

    /**
     * Returns a schema with partition colum of type VARCHAR
     *
     * @return An instance of {@link Schema}
     */
//    public Schema getPartitionSchema()
//    {
//        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
//                .addField(BLOCK_PARTITION_COLUMN_NAME, Types.MinorType.VARCHAR.getType());
//        return schemaBuilder.build();
//    }

    /**
     * Used to get definition (field names, types, descriptions, etc...) of a Table.
     *
     * @param blockAllocator Tool for creating and managing Apache Arrow Blocks.
     * @param request   Provides details on who made the request and which Athena catalog, database, and table they are querying.
     * @return A GetTableResponse which primarily contains:
     * 1. An Apache Arrow Schema object describing the table's columns, types, and descriptions.
     * 2. A Set<String> of partition column names (or empty if the table isn't partitioned).
     * 3. A TableName object confirming the schema and table name the response is for.
     * 4. A catalog name corresponding the Athena catalog that was queried.
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest request) throws Exception
    {
//        TableName tableInfo = request.getTableName();
//        LOGGER.debug("MetadataHandler=GcsMetadataHandler|Method=doGetTable|Message=queryId {}",
//                request.getQueryId());
//        LOGGER.debug("MetadataHandler=GcsMetadataHandler|Method=doGetTable|Message=Schema name {}, table name {}",
//                tableInfo.getSchemaName(), tableInfo.getTableName());
//        datasource.loadAllTables(tableInfo.getSchemaName());
//        LOGGER.debug(MessageFormat.format("Running doGetTable for table {0}, in schema {1} ",
//                tableInfo.getTableName(), tableInfo.getSchemaName()));
//        Schema schema = gcsSchemaUtils.buildTableSchema(this.datasource,
//                tableInfo.getSchemaName(),
//                tableInfo.getTableName());
//        Schema partitionSchema = getPartitionSchema();
        ScanOptions options = new ScanOptions(1);
        Schema schema = null;
        try (
                BufferAllocator allocator = new RootAllocator();
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, STORAGE_FILE);
                Dataset dataset = datasetFactory.finish();
                Scanner scanner = dataset.newScan(options);
                ArrowReader reader = scanner.scanBatches()
        ) {
            schema = reader.getVectorSchemaRoot().getSchema();
        }
        return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema);
//        ,
//                partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet()));
    }

    /**
     * Used to get the partitions that must be read from the request table in order to satisfy the requested predicate.
     *
     * @param blockWriter        Used to write rows (partitions) into the Apache Arrow response.
     * @param request            Provides details of the catalog, database, and table being queried as well as any filter predicate.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) throws IOException
    {
        // TODO: no partition for testing Arrow Dataset
//        LOGGER.debug("Handler=GcsMetadataHandler|Method=getPartitions|Message=queryId {}", request.getQueryId());
//        LOGGER.info("readWithConstraint: schema[{}] tableName[{}]", request.getSchema(), request.getTableName());
//        TableName tableName = request.getTableName();
//        String bucketName = null;
//        String objectName = null;
//        Optional<StorageTable> optionalTable = datasource.getStorageTable(tableName.getSchemaName(),
//                tableName.getTableName());
//        if (optionalTable.isPresent()) {
//            StorageTable table = optionalTable.get();
//            bucketName = table.getParameters().get(TABLE_PARAM_BUCKET_NAME);
//            objectName = table.getParameters().get(TABLE_PARAM_OBJECT_NAME);
//        }
//        LOGGER.info("Getting storage table for {} under bucket {}", objectName, bucketName);
//        requireNonNull(bucketName, "Schema + '" + tableName.getSchemaName() + "' not found");
//        requireNonNull(objectName, "Table '" + tableName.getTableName() + "' not found under schema '"
//                + tableName.getSchemaName() + "'");
//
//        List<StoragePartition> partitions = datasource.getStoragePartitions(request.getSchema(), request.getTableName(), request.getConstraints(), bucketName, objectName);
//        LOGGER.info("GcsMetadataHandler.getPartitions() -> Storage partitions:\n{}", partitions);
//        requireNonNull(partitions, "List of partition can't be retrieve from metadata");
//        int counter = 0;
//        for (int i = 0; i < partitions.size(); i++) {
//            StoragePartition storagePartition = partitions.get(i);
//            blockWriter.writeRows((Block block, int rowNum) ->
//            {
//                block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, storagePartition.getLocation());
//                //we wrote 1 row so we return 1
//                return 1;
//            });
//            counter++;
//        }
//        LOGGER.debug("Total partition rows written: {}", counter);
    }

    /**
     * Used to split up the reads required to scan the requested batch of partition(s).
     * <p>
     * Here we execute the read operations based on row offset and limit form particular bucket and file on GCS
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request   Provides details of the catalog, database, table, and partition(s) being queried as well as
     *                  any filter predicate.
     * @return A GetSplitsResponse which primarily contains:
     * 1. A Set<Split> which represent read operations Amazon Athena must perform by calling your read function.
     * 2. (Optional) A continuation token which allows you to paginate the generation of splits for large queries.
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request) throws IOException
    {
//        LOGGER.debug("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=queryId {}", request.getQueryId());
//        String bucketName = "";
//        String objectNames = "";
//        boolean partitioned = false;
//        String partitionBaseObject = null;
//        TableName tableInfo = request.getTableName();
//        LOGGER.debug("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=Schema name{}, table name {}",
//                tableInfo.getSchemaName(), tableInfo.getTableName());
//        datasource.loadAllTables(tableInfo.getSchemaName());
//        Optional<StorageTable> optionalTable = datasource.getStorageTable(tableInfo.getSchemaName(),
//                tableInfo.getTableName());
//        if (optionalTable.isPresent()) {
//            StorageTable table = optionalTable.get();
//            bucketName = table.getParameters().get(TABLE_PARAM_BUCKET_NAME);
//            objectNames = table.getParameters().get(TABLE_PARAM_OBJECT_NAME_LIST);
//            partitioned = Boolean.parseBoolean(table.getParameters().get(IS_TABLE_PARTITIONED));
//            partitionBaseObject = table.getParameters().get(TABLE_PARAM_OBJECT_NAME);
//        }
//        Block blockPartitions = request.getPartitions();
//        LOGGER.info("Block partition @ doGetSplits \n{}", blockPartitions);
//
//        LOGGER.info("Bucket: {}, object name list:\n{}\nPartitioned: {}, base object name: {}", bucketName, objectNames,
//                partitioned, partitionBaseObject);
//        Block partitions = request.getPartitions();
//        LOGGER.info("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=Partition block {}", partitions);
//        LOGGER.info("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=Block partition row count {}",
//                partitions.getRowCount());
//        Set<Split> splits = new HashSet<>();
//        int partitionContd = decodeContinuationToken(request);
//        LOGGER.info("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=Start splitting from position {}",
//                partitionContd);
//        int startSplitIndex = 0; // storageSplitListIndices.get(0);
//        LOGGER.info("Current split start index {}", startSplitIndex);
//        for (int curPartition = 0; curPartition < partitions.getRowCount(); curPartition++) {
//            SpillLocation spillLocation = makeSpillLocation(request);
//            FieldReader reader = blockPartitions.getFieldReader(BLOCK_PARTITION_COLUMN_NAME);
//            reader.setPosition(curPartition);
//            String prefix = String.valueOf(reader.readText());
//            List<StorageSplit> storageSplits = datasource.getSplitsByBucketPrefix(bucketName, prefix,
//                    partitioned, request.getConstraints());
//            printJson(storageSplits, "storageSplits");
//            LOGGER.info("Splitting based on partition at position {}", curPartition);
//            for (StorageSplit split : storageSplits) {
//                String storageSplitJson = splitAsJson(split);
//                LOGGER.info("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=StorageSplit JSO\n{}",
//                        storageSplitJson);
//                Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
//                        .add(BLOCK_PARTITION_COLUMN_NAME, prefix)
//                        .add(TABLE_PARAM_BUCKET_NAME, bucketName)
//                        .add(TABLE_PARAM_OBJECT_NAME_LIST,  split.getFileName())
//                        .add(STORAGE_SPLIT_JSON, storageSplitJson);
//                splits.add(splitBuilder.build());
//                if (splits.size() >= GcsConstants.MAX_SPLITS_PER_REQUEST) {
//                    //We exceeded the number of split we want to return in a single request, return and provide a continuation token.
//                    return new GetSplitsResponse(request.getCatalogName(), splits, String.valueOf(curPartition + 1));
//                }
//            }
//            LOGGER.info("Splits created {}", splits);
//        }

        // TODO: no split for testing Arrow Dataset
        SpillLocation spillLocation = makeSpillLocation(request);
        return new GetSplitsResponse(request.getCatalogName(), Set.of(Split.newBuilder(spillLocation, makeEncryptionKey()).build()), null);
    }

    // helpers
    /**
     * Decodes continuation token (if any)
     *
     * @param request An instance of {@link GetSplitsRequest}
     * @return Continuation token if found, 0 otherwise
     */
    private int decodeContinuationToken(GetSplitsRequest request)
    {
        LOGGER.debug("Decoding ContinuationToken");
        if (request.hasContinuationToken()) {
            LOGGER.debug("Found decoding ContinuationToken: " + request.getContinuationToken());
            return Integer.parseInt(request.getContinuationToken());
        }
        //No continuation token present
        LOGGER.debug("Not decoding ContinuationTokens found. Returning 0");
        return 0;
    }
}
