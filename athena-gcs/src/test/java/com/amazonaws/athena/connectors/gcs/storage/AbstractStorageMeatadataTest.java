/*-
 * #%L
 * Amazon Athena Storage API
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
package com.amazonaws.athena.connectors.gcs.storage;

import com.amazonaws.athena.connectors.gcs.storage.datasource.StorageDatasourceConfig;
import com.amazonaws.athena.connectors.gcs.storage.datasource.StorageDatasourceFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.reflect.Whitebox;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.gcs.storage.StorageConstants.FILE_EXTENSION_ENV_VAR;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({StorageOptions.class, GoogleCredentials.class})
public class AbstractStorageMeatadataTest extends GcsTestBase
{
    public static final String TABLE_OBJECTS = "tableObjects";
    public static final String DATABASE_BUCKETS = "databaseBuckets";
    public static final String STORAGE = "storage";
    public static final String LOADED_ENTITIES_LIST = "loadedEntitiesList";
    public static final String EXTENSION = "extension";
    private static Map<String, String> csvProps;

    AbstractStorageMetadata abstractStorageMetadata;
    static List<String> bucketList = new ArrayList<>();

    @BeforeClass
    public static void setUp()
    {
        csvProps = new HashMap<>();
        csvProps.put(FILE_EXTENSION_ENV_VAR, "csv");
        csvProps.putAll(properties);
        bucketList.add("test");
    }

    @Before
    public void setUpTest()
    {
        abstractStorageMetadata = PowerMockito.mock(AbstractStorageMetadata.class);
    }

    @Test
    public void testGetAllDatabases()
    {
        Storage storage = mockStorageWithBlobIterator("test");
        PowerMockito.doCallRealMethod()
                .when(abstractStorageMetadata)
                .getAllDatabases();
        Whitebox.setInternalState(abstractStorageMetadata, DATABASE_BUCKETS, new HashMap<>());
        Whitebox.setInternalState(AbstractStorageMetadata.class, STORAGE, storage);
        Whitebox.setInternalState(abstractStorageMetadata, LOADED_ENTITIES_LIST, new ArrayList<>());

        List<String> bList = abstractStorageMetadata.getAllDatabases();
        assertEquals(bList.size(), bucketList.size());
    }

    @Test
    public void testGetAllTables() throws Exception
    {
        PowerMockito.doCallRealMethod()
                .when(abstractStorageMetadata)
                .getAllTables(BUCKET, null, 2);
        Whitebox.setInternalState(abstractStorageMetadata, TABLE_OBJECTS, Map.of("test", Map.of("test", List.of("test"))));
        TableListResult bList = abstractStorageMetadata.getAllTables("test", null, 2);
        assertNotNull(bList);
    }

    @Test
    public void testCheckDatastoreForPagination() throws Exception
    {
        PowerMockito.when(abstractStorageMetadata.loadTablesInternal(anyString(), anyString(), anyInt())).thenReturn("token");
        PowerMockito.doCallRealMethod()
                .when(abstractStorageMetadata)
                .loadTablesWithContinuationToken("test", null, 2);
        Whitebox.setInternalState(abstractStorageMetadata, DATABASE_BUCKETS, Map.of("test", "test"));
        Whitebox.setInternalState(abstractStorageMetadata, "datasourceConfig", new StorageDatasourceConfig().credentialsJson(gcsCredentialsJson).properties(properties));
        String token = abstractStorageMetadata.loadTablesWithContinuationToken("test", null, 2);
        assertNull(token);
    }

//    @Test
//    public void testLoadAllTables() throws IOException
//    {
//        PowerMockito.doCallRealMethod()
//                .when(abstractStorageMetadata)
//                .loadAllTables(BUCKET);
//        Whitebox.setInternalState(abstractStorageMetadata, TABLE_OBJECTS, Map.of("test", Map.of("test", List.of("test"))));
//
//        List<StorageObject> bList = abstractStorageMetadata.loadAllTables(BUCKET);
//        assertNotNull(bList);
//    }
//
//    @Test
//    public void testCheckDatastoreForAll() throws Exception
//    {
//        mockStorageWithInputStream(BUCKET, CSV_FILE);
//        StorageDatasource csvDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, csvProps);
//        csvDatasource.checkDatastoreForDatabase(BUCKET);
//    }
//
//    @Test
//    public void testGetStorageTable() throws Exception
//    {
//        mockStorageWithInputStream(BUCKET, CSV_FILE);
//        StorageDatasource csvDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, csvProps);
//        assertNotNull(csvDatasource.getStorageTable(BUCKET, "dimeemployee"));
//    }
//
//    @Test
//    public void testLoadTablesInternal() throws IOException
//    {
//        PowerMockito.when(storage.getObjectNames(anyString(), anyString(), anyInt())).thenReturn(PagedObject.builder().fileNames(bucketList).nextToken(null).build());
//
//        PowerMockito.doCallRealMethod()
//                .when(abstractStorageMetadata)
//                .loadTablesInternal("test", "null", 2);
//
//        Whitebox.setInternalState(abstractStorageMetadata, TABLE_OBJECTS, new HashMap<>());
//        Whitebox.setInternalState(abstractStorageMetadata, DATABASE_BUCKETS, Map.of("test", "test"));
//        Whitebox.setInternalState(abstractStorageMetadata, STORAGE, storage);
//        Whitebox.setInternalState(abstractStorageMetadata, LOADED_ENTITIES_LIST, List.of(new AbstractStorageDatasource.LoadedEntities("test")));
//        String token = abstractStorageMetadata.loadTablesInternal("test", "null", 2);
//        assertNull(token);
//    }
//
//    @Test(expected = DatabaseNotFoundException.class)
//    public void testLoadTablesInternalException() throws IOException
//    {
//        PowerMockito.doCallRealMethod()
//                .when(abstractStorageMetadata)
//                .loadTablesInternal("test", null, 2);
//        PowerMockito.when(abstractStorageMetadata.convertBlobsToTableObjectsMap(Mockito.any(), Mockito.any())).thenReturn(Map.of(new StorageObject("test", "test.csv", false, List.of("test")), List.of("test")));
//
//        Whitebox.setInternalState(abstractStorageMetadata, TABLE_OBJECTS, new HashMap<>());
//        Whitebox.setInternalState(abstractStorageMetadata, DATABASE_BUCKETS, Map.of());
//        Whitebox.setInternalState(abstractStorageMetadata, STORAGE, storage);
//        Whitebox.setInternalState(abstractStorageMetadata, LOADED_ENTITIES_LIST, List.of(new AbstractStorageDatasource.LoadedEntities("test")));
//        abstractStorageMetadata.loadTablesInternal("test", null, 2);
//    }
//
//    @Test(expected = DatabaseNotFoundException.class)
//    public void testLoadTablesInternalWithoutTokenException() throws IOException
//    {
//        PowerMockito.doCallRealMethod()
//                .when(abstractStorageMetadata)
//                .loadTablesInternal("test");
//        PowerMockito.when(abstractStorageMetadata.convertBlobsToTableObjectsMap(Mockito.any(), Mockito.any())).thenReturn(Map.of(new StorageObject("test", "test.csv", false, List.of("test")), List.of("test")));
//
//        Whitebox.setInternalState(abstractStorageMetadata, TABLE_OBJECTS, new HashMap<>());
//        Whitebox.setInternalState(abstractStorageMetadata, DATABASE_BUCKETS, Map.of("test1", "test"));
//        Whitebox.setInternalState(abstractStorageMetadata, STORAGE, storage);
//        Whitebox.setInternalState(abstractStorageMetadata, LOADED_ENTITIES_LIST, List.of(new AbstractStorageDatasource.LoadedEntities("test")));
//        abstractStorageMetadata.loadTablesInternal("test");
//    }
//
//    @Test
//    public void testLoadTablesInternalWithoutToken() throws IOException
//    {
//        PowerMockito.doCallRealMethod()
//                .when(abstractStorageMetadata)
//                .loadTablesInternal("test");
//        PowerMockito.when(abstractStorageMetadata.convertBlobsToTableObjectsMap(Mockito.any(), Mockito.any())).thenReturn(Map.of(new StorageObject("test", "test.csv", false, List.of("test")), List.of("test")));
//
//        Whitebox.setInternalState(abstractStorageMetadata, TABLE_OBJECTS, new HashMap<>());
//        Whitebox.setInternalState(abstractStorageMetadata, DATABASE_BUCKETS, Map.of("test", "test"));
//        Whitebox.setInternalState(abstractStorageMetadata, STORAGE, storage);
//        Whitebox.setInternalState(abstractStorageMetadata, LOADED_ENTITIES_LIST, List.of(new AbstractStorageDatasource.LoadedEntities("test")));
//        abstractStorageMetadata.loadTablesInternal("test");
//        verify(abstractStorageMetadata, times(3)).loadTablesInternal("test");
//    }
//
//    @Test
//    public void testConvertBlobsToTableObjectsMap() throws IOException
//    {
//        PowerMockito.doCallRealMethod()
//                .when(abstractStorageMetadata)
//                .convertBlobsToTableObjectsMap(BUCKET, bucketList);
//
//
//        Whitebox.setInternalState(abstractStorageMetadata, TABLE_OBJECTS, new HashMap<>());
//        Whitebox.setInternalState(abstractStorageMetadata, DATABASE_BUCKETS, Map.of("test", "test"));
//        Whitebox.setInternalState(abstractStorageMetadata, EXTENSION, "csv");
//        Whitebox.setInternalState(abstractStorageMetadata, LOADED_ENTITIES_LIST, List.of(new AbstractStorageDatasource.LoadedEntities("test")));
//
//        Map<StorageObject, List<String>> obj = abstractStorageMetadata.convertBlobsToTableObjectsMap(BUCKET, bucketList);
//        assertNotNull(obj);
//    }
//
//    @Test
//    public void testAddTable() throws IOException
//    {
//        HashMap<StorageObject, List<String>> map = new HashMap<>();
//        List<String> sList = new ArrayList<>();
//        sList.add("test");
//        map.put(new StorageObject("test", "test.csv", false, List.of("test")), sList);
//        PowerMockito.doCallRealMethod()
//                .when(abstractStorageMetadata)
//                .addTable("test", "test.csv", map);
//
//        PowerMockito.when(storage.isPartitionedDirectory(anyString(), anyString())).thenReturn(true);
//        Whitebox.setInternalState(abstractStorageMetadata, STORAGE, storage);
//        Whitebox.setInternalState(abstractStorageMetadata, "datasourceConfig", new StorageDatasourceConfig().credentialsJson(gcsCredentialsJson).properties(csvProps));
//        Whitebox.setInternalState(abstractStorageMetadata, EXTENSION, "csv");
//        abstractStorageMetadata.addTable("test", "test.csv", map);
//        verify(abstractStorageMetadata, times(3)).addTable("test","test.csv", map);
//    }
//
//    @Test
//    public void testTablesLoadedForDatabase()
//    {
//        PowerMockito.doCallRealMethod()
//                .when(abstractStorageMetadata)
//                .tablesLoadedForDatabase("test");
//
//        Whitebox.setInternalState(abstractStorageMetadata, LOADED_ENTITIES_LIST, List.of(new AbstractStorageDatasource.LoadedEntities("test")));
//        boolean st = abstractStorageMetadata.tablesLoadedForDatabase("test");
//        assertFalse(st);
//    }
//
//    @Test(expected = DatabaseNotFoundException.class)
//    public void testTablesLoadedForDatabaseException()
//    {
//        PowerMockito.doCallRealMethod()
//                .when(abstractStorageMetadata)
//                .tablesLoadedForDatabase("test");
//
//        Whitebox.setInternalState(abstractStorageMetadata, LOADED_ENTITIES_LIST, List.of());
//        boolean st = abstractStorageMetadata.tablesLoadedForDatabase("test");
//        assertFalse(st);
//    }
//
//    @Test
//    public void testContainsInvalidExtension()
//    {
//        PowerMockito.doCallRealMethod()
//                .when(abstractStorageMetadata)
//                .containsInvalidExtension("test.csv");
//        Whitebox.setInternalState(abstractStorageMetadata, "datasourceConfig", new StorageDatasourceConfig().credentialsJson(gcsCredentialsJson).properties(csvProps));
//        assertFalse(abstractStorageMetadata.containsInvalidExtension("test.csv"));
//    }
//
//    @Test
//    public void testGetStoragePartitions() throws Exception {
//        Storage storage = mockStorageWithInputStream(BUCKET, CSV_FILE).getStorage();
//        parquetProps.put(FILE_EXTENSION_ENV_VAR, "csv");
//        List<StorageSplit> splits = new ArrayList<>();
//        String[] fileNames = {CSV_FILE};
//        for (String fileName : fileNames) {
//            splits.addAll(CsvSplitUtil.getStorageSplitList(99, fileName, 100));
//        }
//        StorageDatasource csvDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
//        csvDatasource.loadAllTables(BUCKET);
//        AthenaReadRecordsRequest recordsRequest = buildReadRecordsRequest(Map.of(),
//                BUCKET, CSV_TABLE, splits.get(0), false);
//        csvDatasource.getStoragePartitions(recordsRequest.getSchema(),
//                recordsRequest.getTableName(), recordsRequest.getConstraints(), BUCKET,
//                CSV_FILE);
//    }

}
