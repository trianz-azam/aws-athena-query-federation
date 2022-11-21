package com.amazonaws.athena.connectors.gcs;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import org.apache.arrow.gandiva.evaluator.Projector;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Optional;

public class GcsDatasetFilterUtil
{
    private GcsDatasetFilterUtil()
    {
    }

    public static Optional<Projector> constraintEvaluator(Schema consoleSchema, Schema originalSchema,
                                                         TableName tableInfo, Constraints constraints)
    {
        return Optional.empty();
    }

}
