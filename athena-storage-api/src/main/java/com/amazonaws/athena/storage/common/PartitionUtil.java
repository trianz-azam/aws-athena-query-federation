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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.regex.Pattern;

import static com.amazonaws.athena.storage.io.GcsIOUtil.getFolderName;

public class PartitionUtil
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUtil.class);

    private PartitionUtil()
    {
    }

    public static boolean isPartitionFolder(String folderName)
    {
        String simpleName = getFolderName(folderName);
        return FieldValuePatternMatcher.matches(simpleName);
    }

    public static Optional<FieldValue> getPartitionFieldValue(String folderName)
    {
        String simpleName = getFolderName(folderName);
        LOGGER.info("Creating FieldValue from partition folder {}" + simpleName);
        return FieldValue.from(simpleName);
    }

    private static class FieldValuePatternMatcher
    {
        private static final Pattern FIELD_EQUAL_VALUE_PATTERN_SINGLE_QUOTED_PATTERN = Pattern.compile("([a-zA-Z0-9]+)=\'(.*?)(?<!\\\\)\'");
        private static final Pattern FIELD_EQUAL_VALUE_PATTERN_DOUBLE_QUOTED_PATTERN = Pattern.compile("([a-zA-Z0-9]+)=\"(.*?)(?<!\\\\)\"");
        private static final Pattern FIELD_EQUAL_VALUE_PATTERN_DOUBLE_NO_QUOTED_PATTERN = Pattern.compile("([a-zA-Z0-9]+)=(.*?)(?<!\\\\)");

        public static boolean matches(String maybeFieldValue)
        {
            return matchesSingleQuoted(maybeFieldValue)
                    || matchesDoubleQuoted(maybeFieldValue)
                    || matchesUnQuoted(maybeFieldValue);
        }

        private static boolean matchesSingleQuoted(String maybeFieldValue)
        {
            return FIELD_EQUAL_VALUE_PATTERN_SINGLE_QUOTED_PATTERN.matcher(maybeFieldValue).matches();
        }

        private static boolean matchesDoubleQuoted(String maybeFieldValue)
        {
            return FIELD_EQUAL_VALUE_PATTERN_DOUBLE_QUOTED_PATTERN.matcher(maybeFieldValue).matches();
        }

        private static boolean matchesUnQuoted(String maybeFieldValue)
        {
            return FIELD_EQUAL_VALUE_PATTERN_DOUBLE_NO_QUOTED_PATTERN.matcher(maybeFieldValue).matches();
        }
    }
}
