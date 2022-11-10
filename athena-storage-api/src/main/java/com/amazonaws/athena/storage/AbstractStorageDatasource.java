/*-
 * #%L
 * athena-storage-api
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
package com.amazonaws.athena.storage;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.storage.common.FieldValue;
import com.amazonaws.athena.storage.common.FilterExpression;
import com.amazonaws.athena.storage.common.PagedObject;
import com.amazonaws.athena.storage.common.PartitionUtil;
import com.amazonaws.athena.storage.common.StorageObject;
import com.amazonaws.athena.storage.common.StorageObjectSchema;
import com.amazonaws.athena.storage.common.StoragePartition;
import com.amazonaws.athena.storage.common.StorageProvider;
import com.amazonaws.athena.storage.datasource.StorageDatasourceConfig;
import com.amazonaws.athena.storage.datasource.exception.DatabaseNotFoundException;
import com.amazonaws.athena.storage.datasource.exception.UncheckedStorageDatasourceException;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazonaws.athena.storage.StorageConstants.BLOCK_PARTITION_COLUMN_NAME;
import static com.amazonaws.athena.storage.StorageConstants.IS_TABLE_PARTITIONED;
import static com.amazonaws.athena.storage.StorageConstants.STORAGE_PROVIDER_ENV_VAR;
import static com.amazonaws.athena.storage.StorageConstants.TABLE_PARAM_BUCKET_NAME;
import static com.amazonaws.athena.storage.StorageConstants.TABLE_PARAM_OBJECT_NAME;
import static com.amazonaws.athena.storage.StorageConstants.TABLE_PARAM_OBJECT_NAME_LIST;
import static com.amazonaws.athena.storage.StorageUtil.getValidEntityName;
import static com.amazonaws.athena.storage.StorageUtil.getValidEntityNameFromFile;
import static com.amazonaws.athena.storage.StorageUtil.tableNameFromFile;
import static com.amazonaws.athena.storage.io.GcsIOUtil.containsExtension;
import static com.amazonaws.athena.storage.io.GcsIOUtil.getFolderName;
import static java.util.Objects.requireNonNull;

public abstract class AbstractStorageDatasource implements StorageDatasource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStorageDatasource.class);

    protected final String extension;
    protected final StorageDatasourceConfig datasourceConfig;
    protected final Map<String, String> databaseBuckets = new HashMap<>();
    protected final Map<String, Map<StorageObject, List<String>>> tableObjects = new HashMap<>();
    protected boolean storeCheckingComplete = false;
    protected List<LoadedEntities> loadedEntitiesList = new ArrayList<>();
    protected StorageProvider storageProvider;
    protected String baseObjectName;

    /**
     * Instantiate a storage data source object with provided config
     *
     * @param config An instance of GcsDatasourceConfig that contains necessary properties for instantiating an appropriate data source
     * @throws IOException If occurs during initializing input stream with GCS credential JSON
     */
    protected AbstractStorageDatasource(StorageDatasourceConfig config) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException
    {
        this.datasourceConfig = requireNonNull(config, "ObjectStorageMetastoreConfig is null");
        requireNonNull(config.credentialsJson(), "GCS credential JSON is null");
        requireNonNull(config.properties(), "Environment variables were null");
        this.extension = requireNonNull(config.extension(), "File extension is null");
        loadStorageProvider();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsInvalidExtension(String objectName)
    {
        return containsExtension(objectName)
                && !objectName.endsWith(datasourceConfig.extension());
    }

    /**
     * Return a list of Field instances with field name and field type (Arrow type)
     *
     * @param bucketName  Name of the bucket
     * @param objectNames Name of the file in the specified bucket
     * @return List of field instances
     * @throws IOException Raises if any occurs
     */
    protected abstract List<Field> getTableFields(String bucketName, List<String> objectNames) throws IOException;

    /**
     * Returns a list of all buckets from a cloud storage as databases
     *
     * @return List of database names
     */
    @Override
    public List<String> getAllDatabases()
    {
        List<String> buckets = storageProvider.getAllBuckets();
        for (String bucketName : buckets) {
            String validName = getValidEntityName(bucketName);
            if (!databaseBuckets.containsKey(validName)) {
                loadedEntitiesList.add(new LoadedEntities(validName));
                databaseBuckets.put(validName, bucketName);
            }
        }
        return ImmutableList.copyOf(databaseBuckets.keySet());
    }

    /**
     * List all tables in a database
     *
     * @param databaseName Name of the database
     * @param nextToken    Token for the next page token, may be null
     * @param pageSize     Size of the page (number of tables per table)
     * @return List of all tables under the database
     */
    @Override
    public synchronized TableListResult getAllTables(String databaseName, String nextToken, int pageSize) throws IOException
    {
        String currentNextToken = null;
        if (!storeCheckingComplete
                || !tablesLoadedForDatabase(databaseName)) {
            currentNextToken = this.loadTablesWithContinuationToken(databaseName, nextToken, pageSize);
        }
        LOGGER.info("tableObjects:\n{}", tableObjects);
        List<StorageObject> tables = List.copyOf(tableObjects.getOrDefault(databaseName, Map.of()).keySet());
        return new TableListResult(tables, currentNextToken);
    }

    /**
     * Loads Tables with continuation token from a specific database (bucket). It looks whether the database exists, if it does, it loads all
     * the tables (files) in it based on extension specified in the environment variables. It loads tables from the underlying storage provider with a
     * token and page size (e.g., 10) until all tables (files) are loaded.
     *
     * @param databaseName For which datastore will be checked
     * @param nextToken    Next token for retrieve next page of table list, may be null
     * @param pageSize     Size of the page in each load with token
     */
    @Override
    public synchronized String loadTablesWithContinuationToken(String databaseName, String nextToken, int pageSize) throws IOException
    {
        if (!checkBucketExists(databaseName)) {
            return null;
        }
        String currentNextToken = loadTablesInternal(databaseName, nextToken, pageSize);
        storeCheckingComplete = currentNextToken == null;
        return currentNextToken;
    }

    /**
     * List all tables in a database
     *
     * @param databaseName Name of the database
     * @return List of all tables under the database
     */
    @Override
    public List<StorageObject> loadAllTables(String databaseName) throws IOException
    {
        checkMetastoreForAll(databaseName);
        return List.copyOf(tableObjects.getOrDefault(databaseName, Map.of()).keySet());
    }

    /**
     * Checks datastore for a specific database (bucket). It looks whether the database exists, if it does, it loads all
     * the tables (files) in it based on extension specified in the environment variables
     *
     * @param database For which datastore will be checked
     */
    @Override
    public void checkMetastoreForAll(String database) throws IOException
    {
        if (checkBucketExists(database)) {
            loadTablesInternal(database);
            storeCheckingComplete = true;
        }
    }

    /**
     * Returns a storage object (file) as a DB table with field names and associated file type
     *
     * @param databaseName Name of the database
     * @param tableName    Name of the table
     * @return An instance of {@link StorageTable} with column metadata
     */
    @Override
    public synchronized Optional<StorageTable> getStorageTable(String databaseName, String tableName) throws IOException
    {
        if (!storeCheckingComplete) {
            this.checkMetastoreForAll(databaseName);
        }
        String bucketName = databaseBuckets.get(databaseName);
        if (bucketName == null) {
            throw new RuntimeException("ObjectStorageHiveMetastore.getTable: bucket null does not exist");
        }
        LOGGER.info("Resolving Table {} under the schema {}", tableObjects, databaseName);
        Map<StorageObject, List<String>> objectNameMap = tableObjects.get(databaseName);
        if (objectNameMap != null && !objectNameMap.isEmpty()) {
            Optional<StorageObject> optionalStorageObjectKey = findStorageObjectKey(tableName, databaseName);
            if (optionalStorageObjectKey.isPresent()) {
                StorageObject key = optionalStorageObjectKey.get();
                List<String> objectNames = objectNameMap.get(key);
                if (objectNames != null) {
                    StorageTable table = StorageTable.builder()
                            .setDatabaseName(databaseName)
                            .setTableName(tableName)
                            .partitioned(key.isPartitioned())
                            .setParameter(TABLE_PARAM_BUCKET_NAME, bucketName)
                            .setParameter(TABLE_PARAM_OBJECT_NAME, key.getObjectName())
                            .setParameter(IS_TABLE_PARTITIONED, key.isPartitioned() ? "true" : "false")
                            .setParameter(TABLE_PARAM_OBJECT_NAME_LIST, String.join(",", objectNames))
                            .setFieldList(getTableFields(bucketName, objectNames))
                            .build();
                    return Optional.of(table);
                }
            }
            LOGGER.info("No file(s) found for table {} in the schema {}", tableName, databaseName);
        }
        return Optional.empty();
    }

    @Override
    public List<StoragePartition> getStoragePartitions(Schema schema, TableName tableInfo, Constraints constraints,
                                                       String bucketName, String objectName) throws IOException
    {
        LOGGER.info("Retrieving partitions for object {}, under bucket {}", objectName, bucketName);
        requireNonNull(bucketName, "Bucket name was null");
        requireNonNull(objectName, "objectName name was null");
        List<StoragePartition> storagePartitions = new ArrayList<>();
        if (storageProvider.isDirectory(bucketName, objectName)) {
            LOGGER.info("Object {} under buket {} is a directory", objectName, bucketName);
            if (storageProvider.isPartitionedDirectory(bucketName, objectName)) {
                LOGGER.info("Folder {} under buket {} is  partitioned", objectName, bucketName);
                addPartitionsRecurse(bucketName, objectName, true, schema, tableInfo, constraints,
                        storagePartitions);
            }
        }
        else {
            LOGGER.info("Folder {} under buket {} is NOT partitioned", objectName, bucketName);
            // A file (aka Table) under a non-partitioned bucket/folder
            StoragePartition partition = StoragePartition.builder()
                    .objectNames(List.of(objectName))
                    .location(objectName)
                    .bucketName(bucketName)
                    .build();
            return List.of(partition);
        }
        return storagePartitions;
    }

    @Override
    public StorageProvider getStorageProvider()
    {
        return this.storageProvider;
    }

    @Override
    public List<StoragePartition> getByObjectNameInBucket(String objectName, String bucketName, Schema schema,
                                                          TableName tableInfo, Constraints constraints) throws IOException
    {
        LOGGER.info("Retrieving nested object in partition for object {} under the bucket {}", objectName, bucketName);
        return this.getStoragePartitions(schema, tableInfo, constraints, bucketName, objectName);
    }

    /**
     * Returns the size of records per split
     *
     * @return Size of records per split
     */
    public abstract int recordsPerSplit();

    /**
     * Loads all tables for the given database ana maintain a references of actual bucket and file names
     *
     * @param databaseName Name of the database
     * @param nextToken    Next token to meta-data pagination
     * @param pageSize     Number of table per page when tokenized for pagination
     * @return Next token to retrieve next page
     */
    protected String loadTablesInternal(String databaseName, String nextToken, int pageSize) throws IOException
    {
        requireNonNull(databaseName, "Database name was null");
        String bucketName = databaseBuckets.get(databaseName);
        if (bucketName == null) {
            throw new DatabaseNotFoundException("Bucket null does not exist for Database " + databaseName);
        }
        PagedObject pagedObject = storageProvider.getObjectNames(bucketName, nextToken, pageSize);
        // in the following Map, the key is the Table name, and the value is a list of String contains one or more objects (file)
        // when the file_pattern environment variable is set, usually list String may contain more than one object
        // And in case a table consists of multiple objects, during read it read data from all the objects
        Map<StorageObject, List<String>> objectNameMap = convertBlobsToTableObjectsMap(bucketName, pagedObject.getFileNames());
        tableObjects.put(databaseName, objectNameMap);
        String currentNextToken = pagedObject.getNextToken();
        if (currentNextToken == null) {
            markEntitiesLoaded(databaseName);
        }
        return currentNextToken;
    }

    /**
     * Loads all tables for the given database without pagination
     *
     * @param databaseName Name of the database
     */
    protected void loadTablesInternal(String databaseName) throws IOException
    {
        requireNonNull(databaseName, "Database name was null");
        String bucketName = databaseBuckets.get(databaseName);
        if (bucketName == null) {
            throw new DatabaseNotFoundException("Bucket null does not exist for Database " + databaseName);
        }
        List<String> fileNames = storageProvider.getObjectNames(bucketName);
        Map<StorageObject, List<String>> objectNameMap = convertBlobsToTableObjectsMap(bucketName, fileNames);
        tableObjects.put(databaseName, objectNameMap);
        markEntitiesLoaded(databaseName);
    }

    protected Map<StorageObject, List<String>> convertBlobsToTableObjectsMap(String bucketName, List<String> fileNames) throws IOException
    {
        Map<StorageObject, List<String>> objectNameMap = new HashMap<>();
        for (String fileName : fileNames) {
            if (!isExtensionCheckMandatory()) {
                addTable(bucketName, fileName, objectNameMap);
            }
            else if (fileName.toLowerCase(Locale.ROOT).endsWith(extension.toLowerCase(Locale.ROOT))) {
                addTable(bucketName, fileName, objectNameMap);
            }
        }
        return objectNameMap;
    }

    /**
     * Adds a table with multi parts files. This will maintain a list of multipart files
     *
     * @param objectName Name of the object in storage (file)
     * @param tableMap   Name of the table under which we'll maintain a list of files fall under the pattern
     */
    protected void addTable(String bucketName, String objectName,
                            Map<StorageObject, List<String>> tableMap) throws IOException
    {
        if (containsInvalidExtension(objectName)) {
            LOGGER.info("The object {} contains invalid extension. Expected extension {}", objectName, extension);
            return;
        }
        boolean isPartitionedTable = storageProvider.isPartitionedDirectory(bucketName, objectName);
        LOGGER.info("Adding table for object {}, under bucket {}, and is partitioned? {}", objectName, bucketName, isPartitionedTable);
        String strLowerObjectName = objectName.toLowerCase(Locale.ROOT);
        String tableName = objectName;
        if (isPartitionedTable) {
            tableName = getFolderName(tableName);
        }
        else {
            tableName = tableNameFromFile(tableName, extension);
        }
        LOGGER.info("Table for the object {} under the bucket {} is {}", objectName, bucketName, tableName);

        if (!isPartitionedTable
                // is a directory
                && (objectName.endsWith("/") || storageProvider.isDirectory(bucketName, objectName))) {
            // we're currently not supporting un-partitioned folder to traverse for tables
            return;
        }

        if (strLowerObjectName.endsWith(extension.toLowerCase(Locale.ROOT))
                || !isPartitionedTable
                || isSupported(bucketName, objectName)) {
            StorageObject storageObject = StorageObject.builder()
                    .setTabletName(getValidEntityNameFromFile(tableName, this.extension))
                    .setObjectName(objectName)
                    .setPartitioned(isPartitionedTable)
                    .build();
            tableMap.computeIfAbsent(storageObject,
                    files -> new ArrayList<>()).add(objectName);
        }
        else if (storageProvider.isPartitionedDirectory(bucketName, objectName)) {
            List<String> fileNames = storageProvider.getLeafObjectsByPartitionPrefix(bucketName, objectName, 1);
            if (fileNames.isEmpty()) {
                LOGGER.info("No files found under partitioned table {}", tableName);
            }
            else {
                LOGGER.info("Following files are found found under partitioned table {}\n{}", tableName, fileNames);
            }
            StorageObject storageObject = StorageObject.builder()
                    .setTabletName(getValidEntityNameFromFile(tableName, this.extension))
                    .setObjectName(objectName)
                    .setPartitioned(isPartitionedTable)
                    .build();
            tableMap.computeIfAbsent(storageObject,
                    files -> new ArrayList<>()).addAll(fileNames);
        }
        LOGGER.info("After adding table tableMap\n{}", tableMap);
    }

    /**
     * Determines whether tables are loaded for a given database
     *
     * @param database Name of the database
     * @return True if tables are loaded for the database, false otherwise
     */
    protected boolean tablesLoadedForDatabase(String database)
    {
        Optional<LoadedEntities> optionalLoadedEntities = loadedEntitiesList.stream()
                .filter(entity -> entity.databaseName.equalsIgnoreCase(database))
                .findFirst();
        if (optionalLoadedEntities.isEmpty()) {
            throw new DatabaseNotFoundException("Schema " + database + " not found");
        }
        LoadedEntities entities = optionalLoadedEntities.get();
        return entities.tablesLoaded;
    }

    /**
     * Inner class for keep track whether all tables are loaded for a given table
     */
    protected static class LoadedEntities
    {
        private final String databaseName;
        private boolean tablesLoaded;

        public LoadedEntities(String databaseName)
        {
            this.databaseName = databaseName;
        }
    }

    // inner class
    private static class ObjectStoragePartition
    {
        private String baseObject;
        private boolean directory;
        private List<StoragePartition> partitions;

        public ObjectStoragePartition(String baseObject, boolean directory, List<StoragePartition> partitions)
        {
            this.baseObject = baseObject;
            this.directory = directory;
            this.partitions = partitions;
        }

        public String getBaseObject()
        {
            return baseObject;
        }

        public boolean isDirectory()
        {
            return directory;
        }

        public List<StoragePartition> getPartitions()
        {
            return partitions;
        }

        @Override
        public String toString()
        {
            return "ObjectStoragePartition{" +
                    "baseObject='" + baseObject + '\'' +
                    ", directory=" + directory +
                    ", partitions=" + partitions +
                    '}';
        }
    }

    // helpers
    /**
     * Determines whether a bucket exists for the given database name
     *
     * @param databaseName Name of the database
     * @return True if a bucket exists for the database, false otherwise
     */
    private boolean checkBucketExists(String databaseName)
    {
        if (storeCheckingComplete
                && tablesLoadedForDatabase(databaseName)) {
            return true;
        }
        String bucketName = databaseBuckets.get(databaseName);
        if (bucketName == null) {
            getAllDatabases();
        }
        bucketName = databaseBuckets.get(databaseName);
        if (bucketName == null) {
            throw new DatabaseNotFoundException("No bucket founds for schema '" + databaseName + "'");
        }
        return true;
    }

    // helpers
    /**
     * Mark whether all tables (entities) are loaded maintained in a LoadedEntities object
     *
     * @param database Name of the database to mark all of its tables are loaded
     */
    private void markEntitiesLoaded(String database)
    {
        Optional<LoadedEntities> optionalLoadedEntities = loadedEntitiesList.stream()
                .filter(entity -> entity.databaseName.equalsIgnoreCase(database))
                .findFirst();
        if (optionalLoadedEntities.isEmpty()) {
            throw new DatabaseNotFoundException("Schema " + database + " not found");
        }
        LoadedEntities entities = optionalLoadedEntities.get();
        entities.tablesLoaded = true;
    }

    private void loadStorageProvider() throws InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException
    {
        String providerCodeName = datasourceConfig.getPropertyElseDefault(STORAGE_PROVIDER_ENV_VAR, "gcs");
        Reflections reflections = new Reflections("com.amazonaws.athena.storage");
        Set<Class<? extends StorageProvider>> classes = reflections.getSubTypesOf(StorageProvider.class);
        for (Class<?> storageProviderImpl : classes) {
            Method acceptMethod = storageProviderImpl.getMethod("accept", String.class);
            Object trueOfFalse = acceptMethod.invoke(null, providerCodeName);
            if (((Boolean) trueOfFalse)) {
                Constructor<?> constructor = storageProviderImpl.getConstructor(String.class);
                storageProvider = (StorageProvider) constructor.newInstance(datasourceConfig.credentialsJson());
                break;
            }
        }
        // Still no storage providers found? Then throw run-time exception
        if (storageProvider == null) {
            throw new UncheckedStorageDatasourceException("Storage provider for code name '" + providerCodeName + "' was not found");
        }
    }

    private void addPartitionsRecurse(String bucket, String prefix, boolean isPrefixedRoot, Schema schema, TableName tableName,
                                      Constraints constraints, List<StoragePartition> partitions) throws IOException
    {
        if (baseObjectName == null) {
            Optional<String> optionalBaseObjectName = getBaseName(bucket, prefix);
            if (optionalBaseObjectName.isEmpty()) {
                throw new UncheckedStorageDatasourceException("No file(s) found under bucket '" + bucket + "'"
                        + " inside the folder " + prefix);
            }
            baseObjectName = optionalBaseObjectName.get();
        }
        List<FilterExpression> expressions = getExpressions(bucket, baseObjectName, schema, tableName, constraints,
                Map.of(BLOCK_PARTITION_COLUMN_NAME, BLOCK_PARTITION_COLUMN_NAME));
        LOGGER.info("List of expressions for table {} under bucket {} \n{}", tableName, bucket, expressions);
        StorageObjectSchema objectSchema = getObjectSchema(bucket, baseObjectName);
        List<String> folders = storageProvider.getNestedFolders(bucket, prefix);
        if (folders.isEmpty()) {
            LOGGER.info("Bucket {}, with prefix {} does not contain any nested folders", bucket, partitions);
            return;
        }
        for (String folder : folders) {
            if (PartitionUtil.isPartitionFolder(folder)) {
                Optional<FieldValue> optionalFieldValue = PartitionUtil.getPartitionFieldValue(folder);
                if (optionalFieldValue.isPresent()) {
                    LOGGER.info("Field value {} found for folder {} under bucket {}", optionalFieldValue.get(), folder, bucket);
                    if (matchWithExpression(objectSchema, expressions, optionalFieldValue.get())) {
                        LOGGER.info("Partitioned folder {} is being added under bucket {}", folder, bucket);
                        partitions.add(StoragePartition.builder()
                                .objectNames(List.of(folder))
                                .location(folder)
                                .bucketName(bucket)
                                .build());
                    }
                }
                else {
                    LOGGER.info("NO field values found for folder {} under bucket {}", folder, bucket);
                    partitions.add(StoragePartition.builder()
                            .objectNames(List.of(folder))
                            .location(folder)
                            .bucketName(bucket)
                            .build());
                }
            }
            else {
                LOGGER.info("Folder {} in bucket {} didn't match with pattern", folder, bucket);
                partitions.add(StoragePartition.builder()
                        .objectNames(List.of(folder))
                        .location(folder)
                        .bucketName(bucket)
                        .build());
            }
        }
    }

    private boolean matchWithExpression(StorageObjectSchema objectSchema, List<FilterExpression> expressions,
                                        FieldValue fieldValue)
    {
        if (expressions == null ||  expressions.isEmpty()) {
            LOGGER.info("No expression found to match against field value {}. Returning...", fieldValue);
            return true;
        }
        LOGGER.info("Matching partition folder for field with value {}", fieldValue);
        LOGGER.info("Filter expression field {}", expressions.stream().map(e -> e.columnName()).collect(Collectors.toList()));
        List<FilterExpression> matchedExpression = expressions.stream()
                .filter(expression -> fieldValue.getField().equalsIgnoreCase(expression.columnName()))
                .collect(Collectors.toList());
        boolean matchFound = false;
        if (!matchedExpression.isEmpty()) {
            for (FilterExpression expression : matchedExpression) {
                Object filterValue = expression.filterValue() == null
                        ? "null"
                        : expression.filterValue();
                if (fieldValue.getValue().endsWith(filterValue.toString())) {
                    matchFound = true;
                    break;
                }
            }
        }

        if (!matchFound) {
            long matchCount = objectSchema.getFields().stream()
                    .filter(field -> fieldValue.getField().equalsIgnoreCase(field.getColumnName()))
                    .count();
            matchFound = (matchCount == 0L);
        }
        return matchFound;
    }

    private Optional<StorageObject> findStorageObjectKey(String tableName, String databaseName)
    {
        LOGGER.info("Resolving Table {} under the schema {}", tableObjects, databaseName);
        Map<StorageObject, List<String>> objectNameMap = tableObjects.get(databaseName);
        if (objectNameMap != null && !objectNameMap.isEmpty()) {
            Set<StorageObject> keys = objectNameMap.keySet();
            for (StorageObject storageObject : keys) {
                if (storageObject.getTableName().equals(tableName)) {
                    return Optional.of(storageObject);
                }
            }
        }
        return Optional.empty();
    }

    private boolean isBaseFolderPartitioned()
    {
        return false;
    }
}
