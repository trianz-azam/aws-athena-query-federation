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
package com.amazonaws.athena.connectors.gcs;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.common.collect.Lists;
import org.apache.arrow.gandiva.evaluator.Projector;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.gandiva.expression.TreeNode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;
import java.util.Optional;

public class GcsDatasetFilterUtil
{
    private GcsDatasetFilterUtil()
    {
    }

    public static Optional<Projector> getConstraintEvaluator(Schema originalSchema, Constraints constraints) throws Exception
    {
        Map<String, ValueSet> summary = constraints.getSummary();

        Optional<Field> optionalField = originalSchema.getFields().stream()
                .filter(f -> f.getName().equals("confirmed"))
                .findFirst();

        if (optionalField.isEmpty()) {
            throw new RuntimeException("name field wasn't found");
        }

        Field nameField = optionalField.get();
        TreeNode condition =
                TreeBuilder.makeFunction(
                        "equal",
                        Lists.newArrayList(TreeBuilder.makeField(nameField), TreeBuilder.makeLiteral(262)),
                        new ArrowType.Bool());
        ExpressionTree expr = TreeBuilder.makeExpression(condition, Field.nullable("res", new ArrowType.Bool()));
        Projector projector = Projector.make(originalSchema, Lists.newArrayList(expr));

        return Optional.of(projector);
        // return Optional.empty();
    }
}
