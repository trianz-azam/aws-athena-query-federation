/*-
 * #%L
 * athena-gcs
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;

public class GcsRecordHandler
        extends RecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GcsRecordHandler.class);

    private static final String STORAGE_FILE = "s3://GOOG1EGNWCPMWNY5IOMRELOVM22ZQEBEVDS7NXL5GOSRX6BA2F7RMA6YJGO3Q:haK0skzuPrUljknEsfcRJCYRXklVAh+LuaIiirh1@athena-integ-test-1/bing_covid-19_data.parquet?endpoint_override=https%3A%2F%2Fstorage.googleapis.com";

    private static final String SOURCE_TYPE = "gcs";

    public GcsRecordHandler() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IOException
    {
        this(AmazonS3ClientBuilder.defaultClient(),
                AWSSecretsManagerClientBuilder.defaultClient(),
                AmazonAthenaClientBuilder.defaultClient());
    }

    /**
     * Constructor that provides access to S3, secret manager and athena
     * <p>
     * @param amazonS3       An instance of AmazonS3
     * @param secretsManager An instance of AWSSecretsManager
     * @param amazonAthena   An instance of AmazonAthena
     */
    @VisibleForTesting
    protected GcsRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena amazonAthena) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IOException
    {
        super(amazonS3, secretsManager, amazonAthena, SOURCE_TYPE);
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
    }

    /**
     * Used to read the row data associated with the provided Split.
     *
     * @param spiller            A BlockSpiller that should be used to write the row data associated with this Split.
     *                           The BlockSpiller automatically handles chunking the response, encrypting, and spilling to S3.
     * @param recordsRequest     Details of the read request, including:
     *                           1. The Split - it contains information
     *                           2. The Catalog, Database, and Table the read request is for.
     *                           3. The filtering predicate (if any)
     *                           4. The columns required for projection.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest,
                                      QueryStatusChecker queryStatusChecker) throws Exception
    {
//        printJson(recordsRequest, "ReadRecordsRequest");
//        Split split = recordsRequest.getSplit();
//        TableName tableName = recordsRequest.getTableName();
//        if (this.datasource == null) {
//            throw new RuntimeException("Table " + tableName.getTableName() + " not found in schema "
//                    + tableName.getSchemaName());
//        }
//        LOGGER.debug("RecordHandler=GcsRecordHandler|Method=readWithConstraint|Message=bucketName "
//                + split.getProperty(TABLE_PARAM_BUCKET_NAME) + ", file name "
//                + split.getProperty(TABLE_PARAM_OBJECT_NAME_LIST));
//
//        this.datasource.loadAllTables(tableName.getSchemaName());
//        datasource.readRecords(recordsRequest.getSchema(), recordsRequest.getConstraints(),
//                recordsRequest.getTableName(), recordsRequest.getSplit(), spiller, queryStatusChecker);

        String[] selectedFields = recordsRequest.getSchema().getFields().stream()
                .map(Field::getName).toArray(String[]::new);
        ScanOptions options = new ScanOptions(10_000, Optional.of(selectedFields));
        try (
                BufferAllocator allocator = new RootAllocator();
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, STORAGE_FILE);
                Dataset dataset = datasetFactory.finish();
                Scanner scanner = dataset.newScan(options);
                ArrowReader reader = scanner.scanBatches()
        ) {
            while (reader.loadNextBatch()) {
                try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
                    for (int i = 0; i < root.getRowCount(); i++) {
                        final int index = i;
                        for (FieldVector value : root.getFieldVectors()) {
                            spiller.writeRows((Block block, int rowNum) -> {
                                boolean isMatched = true;
//                                for (Field field : recordsRequest.getSchema().getFields()) {
                                Object val = value.getObject(index);
                                isMatched &= block.offerValue(selectedFields[index], rowNum, val);
                                if (!isMatched) {
                                    return 0;
                                }
//                                }
                                return 1;
                            });
                        }
                    }
                }
            }
        }
    }
}
