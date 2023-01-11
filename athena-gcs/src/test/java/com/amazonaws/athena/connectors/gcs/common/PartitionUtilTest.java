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
package com.amazonaws.athena.connectors.gcs.common;

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.PARTITION_PATTERN_KEY;
import static com.amazonaws.athena.connectors.gcs.GcsTestUtils.createColumn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PartitionUtilTest
{
    private Table table;

    @Before
    public void setup()
    {
        StorageDescriptor storageDescriptor = mock(StorageDescriptor.class);
        when(storageDescriptor.getLocation()).thenReturn("gs://mydatalake1test/birthday/");
        table = mock(Table.class);
        when(table.getStorageDescriptor()).thenReturn(storageDescriptor);

        List<Column> columns = List.of(
                createColumn("year", "bigint"),
                createColumn("month", "int")
        );
        when(table.getPartitionKeys()).thenReturn(columns);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFolderNameRegExPatterExpectException()
    {
        when(table.getParameters()).thenReturn(Map.of(PARTITION_PATTERN_KEY, "year={year}/birth_month{month}/{day}"));
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
    }

    @Test
    public void testFolderNameRegExPatter()
    {
        when(table.getParameters()).thenReturn(Map.of(PARTITION_PATTERN_KEY, "year={year}/birth_month{month}/"));
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
        assertFalse("Expression shouldn't contain a '{' character", optionalRegEx.get().contains("{"));
        assertFalse("Expression shouldn't contain a '}' character", optionalRegEx.get().contains("}"));
    }

    @Test
    public void dynamicFolderExpressionWithDigits()
    {
        // Odd index matches, otherwise doesn't
        List<String> partitionFolders = List.of(
                "state='Tamilnadu'/",
                "year=2000/birth_month10/",
                "zone=EST/",
                "year=2001/birth_month01/",
                "month01/"
        );
        when(table.getParameters()).thenReturn(Map.of(PARTITION_PATTERN_KEY, "year={year}/birth_month{month}/"));
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
        Pattern folderMatchPattern = Pattern.compile(optionalRegEx.get());
        for (int i = 0; i < partitionFolders.size(); i++) {
            String folder = partitionFolders.get(i);
            if (i % 2 > 0) { // Odd, should match
                assertTrue("Folder " + folder + " didn't match with the pattern", folderMatchPattern.matcher(folder).matches());
            } else { // Even shouldn't
                assertFalse("Folder " + folder + " should NOT match with the pattern", folderMatchPattern.matcher(folder).matches());
            }
        }
    }

    @Test
    public void dynamicFolderExpressionWithDefaultsDates()
    {
        // Odd index matches, otherwise doesn't
        List<String> partitionFolders = List.of(
                "year=2000/birth_month10/",
                "creation_dt=2022-12-20/",
                "zone=EST/",
                "creation_dt=2012-01-01/",
                "month01/"
        );
        // mock
        when(table.getParameters()).thenReturn(Map.of(PARTITION_PATTERN_KEY, "creation_dt={creation_dt}/"));
        List<Column> columns = List.of(
                createColumn("creation_dt", "date")
        );
        when(table.getPartitionKeys()).thenReturn(columns);
        // build regex
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
        Pattern folderMatchPattern = Pattern.compile(optionalRegEx.get());
        for (int i = 0; i < partitionFolders.size(); i++) {
            String folder = partitionFolders.get(i);
            if (i % 2 > 0) { // Odd, should match
                assertTrue("Folder " + folder + " didn't match with the pattern", folderMatchPattern.matcher(folder).matches());
            } else { // Even shouldn't
                assertFalse("Folder " + folder + " should NOT match with the pattern", folderMatchPattern.matcher(folder).matches());
            }
        }
    }

    @Test
    public void dynamicFolderExpressionWithQuotedVarchar() // failed
    {
        // Odd index matches, otherwise doesn't
        List<String> partitionFolders = List.of(
                "year=2000/birth_month10/",
                "state='Tamilnadu'/",
                "zone=EST/",
                "state='UP'/",
                "month01/"
        );
        // mock
        when(table.getParameters()).thenReturn(Map.of(PARTITION_PATTERN_KEY, "state='{stateName}'/"));
        List<Column> columns = List.of(
                createColumn("stateName", "string")
        );
        when(table.getPartitionKeys()).thenReturn(columns);
        // build regex
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
        Pattern folderMatchPattern = Pattern.compile(optionalRegEx.get());
        for (int i = 0; i < partitionFolders.size(); i++) {
            String folder = partitionFolders.get(i);
            if (i % 2 > 0) { // Odd, should match
                assertTrue("Folder " + folder + " didn't match with the pattern", folderMatchPattern.matcher(folder).matches());
            } else { // Even shouldn't
                assertFalse("Folder " + folder + " should NOT match with the pattern", folderMatchPattern.matcher(folder).matches());
            }
        }
    }

    @Test
    public void dynamicFolderExpressionWithUnquotedVarchar()
    {
        // Odd index matches, otherwise doesn't
        List<String> partitionFolders = List.of(
                "year=2000/birth_month10/",
                "state=Tamilnadu/",
                "zone=EST/",
                "state=UP/",
                "month01/"
        );
        // mock
        when(table.getParameters()).thenReturn(Map.of(PARTITION_PATTERN_KEY, "state={stateName}/"));
        List<Column> columns = List.of(
                createColumn("stateName", "string")
        );
        when(table.getPartitionKeys()).thenReturn(columns);
        // build regex
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
        Pattern folderMatchPattern = Pattern.compile(optionalRegEx.get());
        for (int i = 0; i < partitionFolders.size(); i++) {
            String folder = partitionFolders.get(i);
            if (i % 2 > 0) { // Odd, should match
                assertTrue("Folder " + folder + " didn't match with the pattern", folderMatchPattern.matcher(folder).matches());
            } else { // Even shouldn't
                assertFalse("Folder " + folder + " should NOT match with the pattern", folderMatchPattern.matcher(folder).matches());
            }
        }
    }

    @Test
    public void testGetHivePartitions() throws ParseException
    {
        String partitionPatten = "year={year}/birth_month{month}/";
        when(table.getParameters()).thenReturn(Map.of(PARTITION_PATTERN_KEY, partitionPatten));
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
        List<PartitionColumnData> partitions = PartitionUtil.getStoragePartitions(partitionPatten, "year=2000/birth_month09/", optionalRegEx.get(), table.getPartitionKeys(), table.getParameters());
        assertFalse("List of column prefix is empty", partitions.isEmpty());
        assertEquals("Partition size is more than 2", 2, partitions.size());
        // Assert partition 1
        assertEquals("First hive column name doesn't match", partitions.get(0).getColumnName(), "year");
        assertEquals("First hive column value doesn't match", 2000L, partitions.get(0).getColumnValue());

        // Assert partition 2
        assertEquals("Second hive column name doesn't match", partitions.get(1).getColumnName(), "month");
        assertEquals("Second hive column value doesn't match", 9, partitions.get(1).getColumnValue());
    }

    @Test
    public void testGetHiveNonHivePartitions() throws ParseException
    {
        // mock
        List<Column> columns = List.of(
                createColumn("year", "bigint"),
                createColumn("month", "int"),
                createColumn("day", "int")
        );
        when(table.getPartitionKeys()).thenReturn(columns);

        String partitionPatten = "year={year}/birth_month{month}/{day}";
        when(table.getParameters()).thenReturn(Map.of(PARTITION_PATTERN_KEY, partitionPatten));
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
        List<PartitionColumnData> partitions = PartitionUtil.getStoragePartitions(partitionPatten, "year=2000/birth_month09/12/",
                optionalRegEx.get(), table.getPartitionKeys(), table.getParameters());
        assertFalse("List of column prefix is empty", partitions.isEmpty());
        assertEquals("Partition size is more than 3", 3, partitions.size());
        // Assert partition 1
        assertEquals("First hive column name doesn't match", partitions.get(0).getColumnName(), "year");
        assertEquals("First hive column value doesn't match", 2000L, partitions.get(0).getColumnValue());

        // Assert partition 2
        assertEquals("Second hive column name doesn't match", partitions.get(1).getColumnName(), "month");
        assertEquals("Second hive column value doesn't match", 9, partitions.get(1).getColumnValue());

        // Assert partition 3
        assertEquals("Third hive column name doesn't match", partitions.get(2).getColumnName(), "day");
        assertEquals("Thir hive column value doesn't match", 12, partitions.get(2).getColumnValue());
    }

    @Test
    public void testGetPartitionFolders() throws ParseException
    {
        // re-mock
        List<Column> columns = List.of(
                createColumn("year", "bigint"),
                createColumn("month", "int"),
                createColumn("day", "int")
        );
        when(table.getPartitionKeys()).thenReturn(columns);
        String partitionPattern = "year={year}/birth_month{month}/{day}";
        when(table.getParameters()).thenReturn(Map.of(PARTITION_PATTERN_KEY, partitionPattern));

        // list of folders in a bucket
        List<String> bucketFolders = List.of(
                "year=2000/birth_month09/12/",
                "year=2000/birth_month09/abc",
                "year=2001/birth_month12/20/",
                "year=2001/",
                "year=2000/birth_month09/",
                "year=2000/birth_month/12",
                "stateName=2001/birthMonth11/15/"
        );

        // tests
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue("No regular expression found for the partition pattern", optionalRegEx.isPresent());
        Pattern folderMatchingPattern = Pattern.compile(optionalRegEx.get());
        for (String folder : bucketFolders) {
            if (folderMatchingPattern.matcher(folder).matches()) {
                List<PartitionColumnData> partitions = PartitionUtil.getStoragePartitions(partitionPattern, folder, optionalRegEx.get(), table.getPartitionKeys(), table.getParameters());
                assertFalse("List of storage partitions is empty", partitions.isEmpty());
                assertEquals("Partition size is more than 3", 3, partitions.size());
            }
        }
    }
}
