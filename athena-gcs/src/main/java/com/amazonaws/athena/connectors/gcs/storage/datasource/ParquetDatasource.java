/*-
 * #%L
 * athena-hive
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
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.athena.connectors.gcs.storage.datasource;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.gcs.common.*;
import com.amazonaws.athena.connectors.gcs.storage.AbstractStorageDatasource;

import com.amazonaws.athena.connectors.gcs.storage.StorageSplit;
import com.google.cloud.storage.Storage;
import org.apache.arrow.dataset.file.FileFormat;
import com.amazonaws.athena.connectors.gcs.storage.datasource.exception.UncheckedStorageDatasourceException;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.gcs.common.PartitionUtil.getRootName;
import static java.util.Objects.requireNonNull;


@ThreadSafe
public class ParquetDatasource
        extends AbstractStorageDatasource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetDatasource.class);

//    private StorgeGroupRecordConverter storgeGroupRecordConverter;

    /**
     * This constructor, as of now, is invoked to instantiate an instance of ParquetDatasource reflectively
     *
     * @param gcsCredentialJsonString Google Cloud Storage credential JSON to access GCS
     * @param properties              Map of property/value from lambda environment
     * @throws IOException If any occurs
     */
    @SuppressWarnings("unused")
    public ParquetDatasource(String gcsCredentialJsonString,
                             Map<String, String> properties, String hmacKey, String hmacSecret) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException
    {
        this(new StorageDatasourceConfig()
                .credentialsJson(gcsCredentialJsonString)
                .properties(properties)
                .hmacKey(hmacKey)
                .hmacSecret(hmacSecret));
    }

    /**
     * Instantiates a ParquetDatasource based on properties found in the GcsDatasourceConfig instance, such as
     * file_extension
     *
     * @param config An instance of GcsDatasourceConfig
     * @throws IOException If any occurs
     */
    public ParquetDatasource(StorageDatasourceConfig config) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException
    {
        super(config);
    }

    /**
     * Indicates whether a ths datasource supports grouping of multiple files to form a single table
     *
     * @return This datasource supports reading multiple file to form a single table. So it always returns true
     */
//    @Override
//    public boolean supportsPartitioning()
//    {
//        return true;
//    }
//
//    @Override
//    public List<FilterExpression> getAllFilterExpressions(Constraints constraints, String bucketName, String objectName)
//    {
//        return List.of();
//    }

    @Override
    public boolean isExtensionCheckMandatory()
    {
        return false;
    }


//    @Override
//    public StorageObjectSchema getObjectSchema(String bucket, String objectName) throws IOException
//    {
//        requireNonNull(objectName, "Table " + objectName + " in bucket " + bucket + " was null");
//        InputFile inputFile = storageProvider.getInputFile(bucket, objectName);
//        try (ParquetFileReader reader = new ParquetFileReader(inputFile, ParquetReadOptions.builder().build())) {
//            ParquetMetadata metadata = reader.getFooter();
//            TypeFactory.FieldResolver fieldResolver = TypeFactory.filedResolver(metadata);
//            List<Field> schemaFields = fieldResolver.resolveFields();
//            MessageType messageType = metadata.getFileMetaData().getSchema();
//            List<ColumnDescriptor> columnDescriptors = messageType.getColumns();
//                    List<StorageObjectField> fieldList = new ArrayList<>();
//            for (int i = 0; i < columnDescriptors.size(); i++) {
//                ColumnDescriptor columnDescriptor = columnDescriptors.get(i);
//                fieldList.add(StorageObjectField.builder()
//                                .columnName(columnDescriptor.getPath()[0].toLowerCase())
//                                .columnIndex(i)
//                        .build());
//            }
//            return StorageObjectSchema.builder()
//                    .fields(fieldList)
//                    .baseSchema(schemaFields)
//                    .build();
//        }
//    }

    /**
     * {@inheritDoc}
     * @return
     */
    @Override
    public List<FilterExpression> getExpressions(String bucket, String objectName, Schema schema, TableName tableName, Constraints constraints,
                                                 Map<String, String> partitionFieldValueMap) throws IOException
    {
//        StorageObjectSchema objectSchema = getObjectSchema(bucket, objectName);
//        return new ParquetFilter(objectSchema, partitionFieldValueMap)
//                .evaluator(tableName, partitionFieldValueMap, constraints)
//                .getExpressions();
        return List.of();
    }

    @Override
    public boolean isSupported(String bucket, String objectName) throws IOException
    {
        boolean isWithValidExtension = containsInvalidExtension(objectName);
        LOGGER.debug("File {} is with valid extension? {}", objectName, isWithValidExtension);
        if (!isWithValidExtension) {
            String uri = "file:/opt/example.parquet";
            BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            DatasetFactory factory = new FileSystemDatasetFactory(allocator,
                    NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
            // inspect schema
            Schema schema = factory.inspect();
            if (null != schema)
                return true;
        }
        return false;
    }

//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public Optional<String> getBaseName(String bucket, String objectName)
//    {
//        return storageProvider.getFirstObjectNameRecurse(bucket, objectName);
//    }
//
    @Override
    public List<StorageSplit> getSplitsByBucketPrefix(String bucket, String prefix, boolean partitioned, Constraints constraints) throws IOException
    {
        LOGGER.info("ParquetDatasource.getSplitsByBucketPrefix() -> Prefix: {} in bucket {}", prefix, bucket);
        List<String> fileNames;
        if (partitioned) {
            LOGGER.debug("Location {} is a directory, walking through", prefix);
            TreeTraversalContext context = TreeTraversalContext.builder()
                    .hasParent(true)
                    .maxDepth(0)
                    .storage(storage)
                    .build();
            Optional<StorageNode<String>> optionalRoot = StorageTreeNodeBuilder.buildFileOnlyTreeForPrefix(bucket,
                    getRootName(prefix), prefix, context);
            if (optionalRoot.isPresent()) {
                fileNames = optionalRoot.get().getChildren().stream()
                        .map(node -> node.getPath())
                        .collect(Collectors.toList());
            }
            else {
                LOGGER.debug("Prefix {}'s root  not present", prefix);
                return List.of();
            }
        }
        else {
            fileNames = List.of(prefix);
        }
        List<StorageSplit> splits = new ArrayList<>();
        LOGGER.debug("Splitting based on files {}", prefix);
        for (String fileName : fileNames) {
//            InputFile inputFile = storage.getInputFile(bucket, fileName);
//            LOGGER.debug("Reading Splits from the file {}, under the bucket {}", fileName, bucket);
//            try (ParquetFileReader reader = new ParquetFileReader(inputFile, ParquetReadOptions.builder().build())) {
//                splits.addAll(GcsParquetSplitUtil.getStorageSplitList(fileName,
//                        reader, recordsPerSplit()));
//            }
        }
        return splits;
    }


//    /**
//     * {{@inheritDoc}}
//     */
//    @Override
//    public void readRecords(Schema schema, Constraints constraints, TableName tableInfo,
//                            Split split, BlockSpiller spiller, QueryStatusChecker queryStatusChecker) throws IOException
//    {
//        String databaseName = tableInfo.getSchemaName();
//        if (!storeCheckingComplete) {
//            this.checkDatastoreForDatabase(databaseName);
//        }
//        String bucketName;
//        String fileNames = split.getProperty(TABLE_PARAM_OBJECT_NAME_LIST);
//        requireNonNull(fileNames, "No tables found under schema '" + databaseName + "'");
//        String[] fileNameArray = fileNames.split(",");
//        if (fileNameArray.length == 0) {
//            throw new UncheckedStorageDatasourceException("No tables found under schema '" + databaseName + "'");
//        }
//        bucketName = split.getProperty(TABLE_PARAM_BUCKET_NAME);
//        if (bucketName == null) {
//            throw new UncheckedStorageDatasourceException("No schema '" + databaseName + "' found");
//        }
//        final StorageSplit storageSplit
//                = new ObjectMapper()
//                .readValue(split.getProperty(StorageConstants.STORAGE_SPLIT_JSON).getBytes(StandardCharsets.UTF_8),
//                        StorageSplit.class);
//        LOGGER.debug("Reading records for split {} ", storageSplit);
//        readRecords(schema, tableInfo, split, constraints, bucketName, storageSplit.getFileName(), spiller,
//                queryStatusChecker);
//    }

    /**
     * Return a list of Field instances with field name and field type (Arrow type)
     *
     * @param bucketName  Name of the bucket
     * @param objectNames Name of the file in the specified bucket
     * @return List of field instances
     */
//
//    protected List<Field> getTableFields(String bucketName, List<String> objectNames) throws IOException
//    {
//        LOGGER.info("Retrieving field schema for file(s) {}, under the bucket {}", objectNames, bucketName);
//        requireNonNull(objectNames, "List of tables in bucket " + bucketName + " was null");
//        if (objectNames.isEmpty()) {
//            throw new UncheckedStorageDatasourceException("List of tables in bucket " + bucketName + " was empty");
//        }
//        LOGGER.debug("Inferring field schema based on file {}", objectNames.get(0));
//        String uri = "file:/opt/example.parquet";
//        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
//        DatasetFactory factory = new FileSystemDatasetFactory(allocator,
//                NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
//        // inspect schema
//        return factory.inspect().getFields();
//    }

    /**
     * {{@inheritDoc}}
     *
     * @return
     */
    @Override
    public int recordsPerSplit()
    {
//        return 10_000;
        return Integer.parseInt(datasourceConfig.getPropertyElseDefault("records_per_split", "5000"));
    }

//    // helpers
//    /**
//     * Retrieves records for given constraints using parquet file reader instead of parquet reader
//     *
//     * @param split      An instance of Split that contains information of the file along with group, offset and size information
//     * @param bucketName Name of the bucket
//     * @see org.apache.parquet.hadoop.ParquetFileReader
//     */
//    private void readRecords(Schema schema, TableName tableInfo, Split split, Constraints constraints,
//                             String bucketName, String objectName, BlockSpiller spiller,
//                             QueryStatusChecker queryStatusChecker) throws IOException
//    {
//        Stopwatch timer = Stopwatch.createStarted();
//        final StorageSplit storageSplit
//                = new ObjectMapper()
//                .readValue(split.getProperty(StorageConstants.STORAGE_SPLIT_JSON).getBytes(StandardCharsets.UTF_8),
//                        StorageSplit.class);
//        InputFile inputFile = storageProvider.getInputFile(bucketName, storageSplit.getFileName());
//        try (ParquetFileReader reader = new ParquetFileReader(inputFile, ParquetReadOptions.builder().build())) {
//            MessageType messageType = reader.getFileMetaData().getSchema();
//            Configuration configuration = new Configuration();
//            configuration.set(ReadSupport.PARQUET_READ_SCHEMA, messageType.toString());
//            final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(messageType);
//            List<GroupSplit> groupSplits = storageSplit.getGroupSplits();
//            for (GroupSplit groupSplit : groupSplits) {
//                PageReadStore pages = reader.readRowGroup(groupSplit.getGroupIndex());
//                FilterCompat.Filter filter = FilterCompat.get(page(groupSplit.getRowOffset(), groupSplit.getRowCount()));
//                if (pages != null && queryStatusChecker.isQueryRunning()) {
//                    ParquetFilter parquetFilter = new ParquetFilter(schema, messageType, split);
//                    ConstraintEvaluator evaluator = parquetFilter.evaluator(tableInfo, split, constraints);
//                    LOGGER.debug("Parquet evaluator: {}", evaluator);
//                    storgeGroupRecordConverter = new StorgeGroupRecordConverter(messageType, evaluator);
//                    addRecords(schema, messageType, columnIO.getRecordReader(pages,
//                            storgeGroupRecordConverter, filter), objectName, spiller, queryStatusChecker);
//                }
//            }
//        }
//        finally {
//            timer.stop();
//            LOGGER.debug("Time took to read records: {}", timer.elapsed(TimeUnit.SECONDS));
//        }
//    }
//
//    /**
//     * Add a record when not being filtered
//     *
//     * @param messageType       An instance of Schema with selected fields and associates types
//     * @param groupRecordReader A reader to field/value from Group
//     */
//    private void addRecords(Schema schema, MessageType messageType, RecordReader<Group> groupRecordReader, String partFileName,
//                            BlockSpiller spiller, QueryStatusChecker queryStatusChecker)
//    {
//        Group group;
//            TypeFactory.ValueResolver valueResolver = TypeFactory.valueResolver(messageType);
//        while ((group = groupRecordReader.read()) != null) {
//            if (queryStatusChecker.isQueryRunning()
//                    && (storgeGroupRecordConverter == null || !storgeGroupRecordConverter.shouldSkipCurrent())) {
//                LOGGER.debug("Parquet record group: {}, class name: {}", group, group.getClass().getName());
//                Map<String, Object> record = valueResolver.getRecord(group);
//                record.put(BLOCK_PARTITION_COLUMN_NAME, partFileName);
//                spiller.writeRows((Block block, int rowNum) -> {
//                    boolean isMatched = true;
//                    for (final Field field : schema.getFields()) {
//                        Object fieldValue = record.get(field.getName());
//                        isMatched &= block.offerValue(field.getName(), rowNum, fieldValue);
//                    }
//                    return isMatched ? 1 : 0;
//                });
//            }
//        }
//    }
}
