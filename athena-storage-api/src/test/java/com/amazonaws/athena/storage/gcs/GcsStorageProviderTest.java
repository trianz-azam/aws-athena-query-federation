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
package com.amazonaws.athena.storage.gcs;

import com.amazonaws.athena.storage.StorageDatasource;
import com.amazonaws.athena.storage.common.StoragePartition;
import com.amazonaws.athena.storage.common.StorageProvider;
import com.amazonaws.athena.storage.datasource.StorageDatasourceFactory;
import org.junit.Test;

import javax.xml.crypto.Data;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Scanner;

import static com.amazonaws.athena.storage.StorageConstants.FILE_EXTENSION_ENV_VAR;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GcsStorageProviderTest
{
    private static final String BUCKET = "feroz";

    @Test
    public void testIsPartitionedFolder() throws FileNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException
    {
        StorageDatasource datasource = getTestDataSource("parquet");
        StorageProvider storageProvider = datasource.getStorageProvider();
        boolean isPartitioned = storageProvider.isPartitionedDirectory(BUCKET, "big_data/");
        assertTrue(isPartitioned, "Folder big_data/");
    }


    private StorageDatasource getTestDataSource(String extension) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, FileNotFoundException
    {
        String jsonCredential = new Scanner(new File("/home/mdaliazam/afq/sec/akshay-gcs-creds.json")).useDelimiter("\\Z").next();
        return StorageDatasourceFactory.createDatasource(jsonCredential, Map.of(FILE_EXTENSION_ENV_VAR, extension));
    }
}
