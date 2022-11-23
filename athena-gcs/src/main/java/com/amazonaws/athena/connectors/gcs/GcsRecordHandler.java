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
import org.apache.arrow.gandiva.evaluator.Projector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

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
        System.out.printf("[reader] starting reading...%n");

        copyCertificateToTmpDirectory();
        String uri = "s3://GOOG1EGNWCPMWNY5IOMRELOVM22ZQEBEVDS7NXL5GOSRX6BA2F7RMA6YJGO3Q:haK0skzuPrUljknEsfcRJCYRXklVAh+LuaIiirh1@athena-integ-test-1/bing_covid-19_data.parquet?endpoint_override=https%3A%2F%2Fstorage.googleapis.com";

        ScanOptions options = new ScanOptions(/*batchSize*/ 32768);

        try (
                // Taking an allocator for using direct memory for Arrow Vectors/Arrays.
                // We will use this allocator for filtered result output.
                BufferAllocator allocator = new RootAllocator();

                // DatasetFactory provides a way to inspect a Dataset potential schema before materializing it.
                // Thus, we can peek the schema for data sources and decide on a unified schema.
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                        allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri
                );

                // Creates a Dataset with auto-inferred schema
                Dataset dataset = datasetFactory.finish();

                // Create a new Scanner using the provided scan options.
                // This scanner also contains the arrow schema for the dataset.
                Scanner scanner = dataset.newScan(options);

                // To read Schema and ArrowRecordBatches we need a reader.
                // This reader reads the dataset as a stream of record batches.
                ArrowReader reader = scanner.scanBatches()
        ) {
            System.out.printf("[reader] creating constraint evaluator...%n");

            // We need a mechanism to evaluate a set of expressions against a RecordBatch.
            // The Projector contains the definitions for filter data using the constraints.
            Optional<Projector> constraintEvaluator = GcsDatasetFilterUtil.getConstraintEvaluator(
                reader.getVectorSchemaRoot().getSchema(),
                recordsRequest.getConstraints()
            );
            System.out.printf("[reader] created constraint evaluator...%n");

            // We are loading records batch by batch until we reached at the end.
            while (reader.loadNextBatch()) {
                System.out.printf("[reader] loaded next batch...%n");

                try (
                        // Returns the vector schema root.
                        // This will be loaded with new values on every call to loadNextBatch on the reader.
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();

                        // Taking a fixed width (1 bit) vector of boolean values.
                        // We will pass it to gandiva's Projector that will evaluate the
                        // expressions against to recordBatch
                        // This is a reference about the result of filter application.
                        // By the row index will access it to know the result.
                        BitVector filterResult = new BitVector("", allocator)
                ) {
                    // Allocating memory for the vector,
                    // it is equal to the size of the records in the batch.
                    filterResult.allocateNew(root.getRowCount());

                    // Taking a helper to handle the conversion of VectorSchemaRoot to a ArrowRecordBatch.
                    final VectorUnloader vectorUnloader = new VectorUnloader(root);

                    // We need a holder for the result of applying the Project on the data.
                    List<ValueVector> filterOutput = new ArrayList<>();
                    filterOutput.add(filterResult);

                    // Getting converted ArrowRecordBatch from the helper.
                    try (ArrowRecordBatch batch = vectorUnloader.getRecordBatch()) {
                        System.out.printf("[reader] found batch data...%n");

                        // If we have Project for filter then apply it on the batch records.
                        // We will get the filter result at filterOutput.
                        if (constraintEvaluator.isPresent()) {
                            constraintEvaluator.get().evaluate(batch, filterOutput);
                        }

                        // We will loop on batch records and consider each records to write in spiller.
                        for (int rowIndex = 0; rowIndex < root.getRowCount(); rowIndex++) {
                            if (constraintEvaluator.isPresent()) {
                                // As we have Project evaluator and filtered result,
                                // we will check if it has been qualified to write in to spiller.
                                if (filterResult.getObject(rowIndex)) {
                                    execute(spiller, root.getFieldVectors(), rowIndex);
                                }
                            }
                            else {
                                // As we do not have Project evaluator and no filtered result,
                                // we will directly write to spiller.
                                execute(spiller, root.getFieldVectors(), rowIndex);
                            }
                        }
                    }
                }
            }
        }

        System.out.printf("[reader] ending reading...%n");
    }

    /**
     * We are writing data to spiller. This function received the whole batch
     * along with row index. We will access into batch using the row index and
     * get the record to write into spiller.
     *
     * @param spiller - block spiller
     * @param gcsFieldVectors - the batch
     * @param rowIndex - row index
     * @throws Exception - exception
     */
    private void execute(
            BlockSpiller spiller,
            List<FieldVector> gcsFieldVectors, int rowIndex) throws Exception
    {
        StringBuilder sb = new StringBuilder("[reader] spilling: [");

        spiller.writeRows((Block block, int rowNum) -> {
            boolean isMatched;
            for (FieldVector vector : gcsFieldVectors) {
                // Writing data in spiller for each field.
                isMatched = block.offerValue(vector.getField().getName(), rowNum, vector.getObject(rowIndex));

                // If this field is not qualified we are not trying with next field,
                // just leaving the whole record.
                if (!isMatched) {
                    return 0;
                }
                sb.append(vector.getField().getName()).append(": ").append(vector.getObject(rowIndex)).append(", ");
            }
            return 1;
        });

        sb.append("]").append(" row(").append(rowIndex).append(")");
        System.out.println(sb);
    }

    private void copyCertificateToTmpDirectory() throws Exception
    {
        ClassLoader classLoader = GcsRecordHandler.class.getClassLoader();
        File file = new File(requireNonNull(classLoader.getResource("")).getFile());
        File src = new File(file.getAbsolutePath() + File.separator + "cacert.pem");
        File dest = new File(Paths.get("/tmp").toAbsolutePath() + File.separator  + "cacert.pem");
        Files.copy(src.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
        int x = 0;
    }
}
