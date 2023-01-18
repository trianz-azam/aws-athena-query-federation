/*-
 * #%L
 * athena-gcs
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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
package com.amazonaws.athena.connectors.gcs.common;

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.Table;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.PARTITION_PATTERN_KEY;

public class PartitionUtil
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUtil.class);

    /**
     * Partition pattern regular expression to be used in compiling Pattern
     */
    private static final String PARTITION_PATTERN_REGEX = "\\{(.*?)}";

    /**
     * Pattern from a regular expression that identifies a match in a phrases to see if there is any
     * partition key variable placeholder. A partition key variable placeholder looks something like the following:
     * year={year}/month={month}
     * Here, {year} and {month} are the partition key variable placeholders
     */
    private static final Pattern PARTITION_PATTERN = Pattern.compile(PARTITION_PATTERN_REGEX);

    /**
     * Match any alpha-num characters. Used to match VARCHAR type only partition keys
     */
    private static final String VARCHAR_OR_STRING_REGEX = "([a-zA-Z0-9]+)";

    private PartitionUtil()
    {
    }

    /**
     * Return a list of {@link PartitionColumnData} instances
     *
     * @param table response of get table from AWS Glue
     * @param partitionFolder      partition folder name
     * @return List of {@link PartitionColumnData} instances (column name, and value)
     */
    public static List<PartitionColumnData> getPartitionColumnData(Table table, String partitionFolder)
    {
        Optional<String> optionalFolderRegex = getRegExExpression(table);
        if (optionalFolderRegex.isEmpty()) {
            return List.of();
        }
        Matcher columnNameMatcher = PARTITION_PATTERN.matcher(table.getParameters().get(PARTITION_PATTERN_KEY));
        String folderRegex = optionalFolderRegex.get();
        Pattern folderMatchPattern = Pattern.compile(folderRegex);
        if (!folderMatchPattern.matcher(partitionFolder).matches()) {
            return List.of();
        }
        List<String> columnNames = new ArrayList<>();
        while (columnNameMatcher.find()) {
            columnNames.add(columnNameMatcher.group(1));
        }

        String[] regExParts = folderRegex.split("/");
        String[] folderParts = partitionFolder.split("/");
        List<PartitionColumnData> partitions = new ArrayList<>();
        List<Column> partitionColumns = table.getPartitionKeys();
        if (folderParts.length >= regExParts.length) {
            for (int i = 0; i < regExParts.length; i++) {
                Matcher matcher = Pattern.compile(regExParts[i]).matcher(folderParts[i]);
                if (matcher.matches()) {
                    Optional<PartitionColumnData> optionalStoragePartition = produceStoragePartition(partitionColumns,
                            columnNames.get(i), matcher.group(1));
                    optionalStoragePartition.ifPresent(partitions::add);
                }
            }
        }
        return partitions;
    }

    // helpers
    /**
     * Return a regular expression for partition pattern from AWS Glue. This will dynamically generate a
     * regular expression to match a folder within the GCS to see if the folder conforms with the partition keys
     * already setup in the AWS Glue Table (if any)
     *
     * @param table response of get table from AWS Glue
     * @return optional Sting of regular expression
     */
    protected static Optional<String> getRegExExpression(Table table)
    {
        if (table.getPartitionKeys().isEmpty()) {
            return Optional.empty();
        }
        List<Column> partitionColumns = table.getPartitionKeys();
        validatePartitionColumnTypes(partitionColumns);
        String partitionPattern = table.getParameters().get(PARTITION_PATTERN_KEY);
        // Check to see if there is a partition pattern configured for the Table by the user
        // if not, it returns empty value
        if (partitionPattern == null || partitionPattern.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(partitionPattern.replaceAll(PARTITION_PATTERN_REGEX, VARCHAR_OR_STRING_REGEX));
    }

    /**
     * Validates partition column types. As of now, only VARCHAR (string or varchar in Glue Table)
     * @param columns List of Glue Table's columns
     */
    private static void validatePartitionColumnTypes(List<Column> columns)
    {
        for (Column column : columns) {
            String columnType = column.getType();
            LOGGER.info("validatePartitionColumnTypes - Field type of {} is {}", column.getName(), columnType);
            switch (columnType.toLowerCase()) {
                case "string":
                case "varchar":
                    return;
                default:
                    throw new IllegalArgumentException("Field type '" + columnType + "' is not supported for a partition field in this connector. " +
                            "Supported partition field type is VARCHAR (string or varchar in a Glue Table Schema)");
            }
        }
    }

    /**
     * Return a true when storage partition added successfully
     *
     * @param columns         list of column
     * @param columnName      Name of the partition column
     * @param columnValue     value of partition folder
     * @return boolean flag
     */
    private static Optional<PartitionColumnData> produceStoragePartition(List<Column> columns, String columnName, String columnValue)
    {
        if (columnValue != null && !columnValue.isBlank() && !columns.isEmpty()) {
            for (Column column : columns) {
                if (column.getName().equalsIgnoreCase(columnName)) {
                    return Optional.of(new PartitionColumnData(column.getName(), columnValue));
                }
            }
        }
        throw new IllegalArgumentException("Column '" + columnName + "' is not defined as partition ke in Glue Table");
    }

    /**
     * Determine the partition folder URI based on Table's partition.pattern and value retrieved from partition field reader (form readWithConstraint() method of GcsRecordHandler)
     * For example, for the following partition.pattern of the Glue Table:
     * <p>/folderName1={partitionKey1}</p>
     * And for the following partition row (from getPartitions() method in GcsMetadataHandler):
     * <p>
     *     Partition fields and value:
     *     <ul>
     *         <li>Partition column: folderName1</li>
     *         <li>Partition column value: partitionFieldVale</li>
     *     </ul>
     * </p>
     * when the Table's Location URI is gs://my_table/data/
     * this method will return a URI that refer to the GCS location: gs://my_table/data/folderName1=partitionFieldVale
     * @return Gcs location URI
     */
    public static URI getPartitionFolderLocationUri(Table table, Map<String, FieldReader> fieldReadersMap) throws URISyntaxException
    {
        String locationUri;
        String tableLocation = table.getStorageDescriptor().getLocation();
        String partitionPattern = table.getParameters().get(PARTITION_PATTERN_KEY);
        if (null != partitionPattern) {
            for (Map.Entry<String, FieldReader> field : fieldReadersMap.entrySet()) {
                partitionPattern = partitionPattern.replace("{" + field.getKey() + "}", field.getValue().readObject().toString());
            }
            locationUri = (tableLocation.endsWith("/")
                    ? tableLocation
                    : tableLocation + "/") + partitionPattern;
        }
        else {
            locationUri = tableLocation;
        }
        return new URI(locationUri);
    }
}
