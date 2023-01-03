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
package com.amazonaws.athena.connectors.gcs.filter;

public class EqualsExpression
{

    final Integer columnIndex;

    final String columnName;

    public Object expression;

    /**
     * Constructs this with column index and an expression
     *
     * @param columnIndex      Index of the column
     * @param expression Expression to match
     */
    public EqualsExpression(Integer columnIndex, String columnName, Object expression)
    {
       this.columnIndex = columnIndex;
       this.columnName = columnName;
       this.expression = expression;
    }

    public String columnName()
    {
        return this.columnName;
    }

    /**
     * Applies the value against this expression
     *
     * @param value Column value
     * @return Return if a match found, false otherwise
     */
    public boolean apply(Object value)
    {
        boolean evaluated = false;
        if (expression == null
                && (value == null || value.toString().equals("null") || value.toString().equals("NULL"))) {
            evaluated = true;
        }
        else if (expression != null) {
            evaluated = expression.toString().equals(value.toString());
        }
        return evaluated;
    }

    /**
     * Applies the value to evaluate the expression
     *
     * @param value Value being examined
     * @return True if the expression evaluated to true (matches), false otherwise
     */
    public boolean apply(String value)
    {
        boolean evaluated = false;
        if (expression == null
                && (value == null || value.equals("null") || value.equals("NULL"))) {
            evaluated = true;
        }
        else if (expression != null) {
            evaluated = expression.toString().equals(value);
        }
        return evaluated;
    }
}
