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
package com.amazonaws.athena.storage.common;

import com.amazonaws.athena.storage.StorageDatasource;
import com.amazonaws.athena.storage.datasource.StorageDatasourceFactory;
import com.amazonaws.athena.storage.gcs.io.GcsStorageProvider;
import org.junit.Test;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;

import static com.amazonaws.athena.storage.StorageConstants.FILE_EXTENSION_ENV_VAR;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({GcsStorageProvider.class})
public class StorageNodeTest
{
    private final static String BUCKET = "mydatalake1";

    @Test
    public void testParentPath()
    {
        String[] paths = {
                "birthday/",
                "birthday/year=2000/",
                "birthday/year=2000/birthday.parquet",
                "zipcode/",
                "zipcode/StateName='TamilNadu'/",
                "zipcode/StateName='TamilNadu'/zipcode.parquet",
                "zipcode/StateName='UP'/",
                "zipcode/StateName='UP'/zipcode.parquet",
        };
        for (String path : paths) {
            System.out.println(getParentPath(path));
        }
    }

    @Test
    public void testNodesAreSortedByName() {
        StorageNode<String> root = new StorageNode<>("zipcode", "zipcode/");
        StorageNode<String> child = root.addChild("StateName='UP'", "zipcode/StateName='UP'/");
        child.addChild("D", "D:\\");
        child.addChild("C", "C:\\");
        root.addChild("StateName='Tamil Nadu'", "zipcode/StateName='Tamil Nadu'/");
        printChildrenRecurse(root.getChildren());
    }

    @Test
    public void testStorageTree() throws Exception {
        String[] paths = {
                "birthday/",
                "birthday/year=2000/",
                "birthday/year=2000/birthday.parquet",
                "zipcode/",
                "zipcode/StateName='TamilNadu'/",
                "zipcode/StateName='TamilNadu'/zipcode.parquet",
                "zipcode/StateName='UP'/",
                "zipcode/StateName/",
                "zipcode/StateName='UP'/zipcode.parquet",
                "zipcode/PinCode/"
        };
        TreeTraversalContext context = TreeTraversalContext.builder()
                .hasParent(false)
                .includeFile(false)
                .maxDepth(3)
                .storageDatasource(getTestDataSource("parquet"))
                .build();
        StorageNode<String> root = new StorageNode<>("/", "/");
        for (String data : paths) {
            String[] names = context.normalizePaths(data.split("/"));
            if (names.length == 0 || !root.isChild(names[0])) {
                continue;
            }
            StorageNode<String> parent = root;
            for (int i = 0; i < names.length; i++) {
                if (parent.getPath().equals(names[i])) {
                    continue;
                }
                String path = String.join("/",
                        Arrays.copyOfRange(names, 0, i + 1));
                if (!context.isIncludeFile() && context.isFile(BUCKET, path)) {
                    continue;
                }
                if (context.getPartitionDepth() > -1 && !context.isPartitioned(i, names[i])) {
                    continue;
                }
                Optional<StorageNode<String>> optionalParent = root.findByPath(getParentPath(path));
                if (optionalParent.isPresent()) {
                    parent = optionalParent.get();
                    if (parent.getPath().equals(path)) {
                        continue;
                    }
                }
                parent = parent.addChild(names[i], path);

            }
        }
        printChildrenRecurse(root.getChildren());
    }

    private String getParentPath(String path)
    {
        if (path.endsWith("/") && path.trim().length() == 1) {
            return null;
        }

        if (path.endsWith("/")) {
            path = path.substring(0, path.lastIndexOf("/"));
            if (path.trim().length() == 1) {
                return path;
            }
            return finalParentPath(path);
        }
        else if (path.trim().length() > 0) {
           return finalParentPath(path);
        }
        return null;

    }

    private static String finalParentPath(String path)
    {
        int lastPathSeparatorIndex = path.lastIndexOf("/");
        if (lastPathSeparatorIndex > -1) {
            return path.substring(0, lastPathSeparatorIndex);
        }
        return null;
    }

    private void printChildrenRecurse(Set<StorageNode<String>> children)
    {
        System.out.println(children);
        for (StorageNode<String> node : children) {
            if (!node.isLeaf()) {
                printChildrenRecurse(node.getChildren());
            }
        }
    }

    private StorageDatasource getTestDataSource(final String extension) throws FileNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException
    {
        String jsonCredential = new Scanner(new File("/home/mdaliazam/afq/sec/akshay-gcs-creds.json")).useDelimiter("\\Z").next();
        return StorageDatasourceFactory.createDatasource(jsonCredential, Map.of(FILE_EXTENSION_ENV_VAR, extension));
    }
}
