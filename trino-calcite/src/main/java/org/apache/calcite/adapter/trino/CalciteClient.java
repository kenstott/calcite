/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.trino;

import com.google.inject.Inject;

import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.sql.Connection;
import java.sql.Types;
import java.util.Optional;

import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultCharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longTimestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Math.min;
import static java.lang.String.format;

/**
 * {@link io.trino.plugin.jdbc.JdbcClient} for the generic Calcite JDBC driver.
 *
 * <p>Calcite exposes its data over JDBC using standard {@link java.sql.Types}, so the type
 * mappings here delegate to Trino's {@link io.trino.plugin.jdbc.StandardColumnMappings}. The
 * underlying Calcite adapters (e.g. SharePoint) are full-scan with no predicate/projection
 * pushdown, so Trino reads all rows and filters locally - the default {@link BaseJdbcClient}
 * behaviour (no pushdown) is intentionally retained.
 */
public class CalciteClient
        extends BaseJdbcClient
{
    @Inject
    public CalciteClient(
            BaseJdbcConfig config,
            @ForBaseJdbc ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        // Calcite quotes identifiers with double quotes; this connector is read-oriented, so
        // non-transactional writes (supportsRetries = false).
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(),
                identifierMapping, queryModifier, false);
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(
            ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> forced = getForcedMappingToVarchar(typeHandle);
        if (forced.isPresent()) {
            return forced;
        }

        switch (typeHandle.jdbcType()) {
        case Types.BIT:
        case Types.BOOLEAN:
            return Optional.of(booleanColumnMapping());
        case Types.TINYINT:
            return Optional.of(tinyintColumnMapping());
        case Types.SMALLINT:
            return Optional.of(smallintColumnMapping());
        case Types.INTEGER:
            return Optional.of(integerColumnMapping());
        case Types.BIGINT:
            return Optional.of(bigintColumnMapping());
        case Types.REAL:
            return Optional.of(realColumnMapping());
        case Types.FLOAT:
        case Types.DOUBLE:
            return Optional.of(doubleColumnMapping());
        case Types.NUMERIC:
        case Types.DECIMAL: {
            int precision = typeHandle.columnSize().orElse(MAX_PRECISION);
            int scale = Math.max(typeHandle.decimalDigits().orElse(0), 0);
            if (precision < 1 || precision > MAX_PRECISION) {
                precision = MAX_PRECISION;
            }
            if (scale > precision) {
                scale = precision;
            }
            return Optional.of(decimalColumnMapping(createDecimalType(precision, scale)));
        }
        case Types.CHAR:
        case Types.NCHAR: {
            int size = typeHandle.columnSize().orElse(0);
            if (size <= 0 || size > CharType.MAX_LENGTH) {
                // Unspecified/oversized CHAR: fall back to unbounded varchar rather than fail.
                return Optional.of(varcharColumnMapping(createUnboundedVarcharType(), true));
            }
            return Optional.of(defaultCharColumnMapping(size, true));
        }
        case Types.VARCHAR:
        case Types.NVARCHAR:
        case Types.LONGVARCHAR:
        case Types.LONGNVARCHAR: {
            // Calcite reports an unspecified precision (<= 0) for unbounded VARCHAR; map those and
            // oversized lengths to an unbounded varchar to avoid an invalid VARCHAR length.
            int size = typeHandle.columnSize().orElse(0);
            if (size <= 0 || size > VarcharType.MAX_LENGTH) {
                return Optional.of(varcharColumnMapping(createUnboundedVarcharType(), true));
            }
            return Optional.of(defaultVarcharColumnMapping(size, true));
        }
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
            return Optional.of(varbinaryColumnMapping());
        case Types.DATE:
            return Optional.of(dateColumnMappingUsingLocalDate());
        case Types.TIME:
            return Optional.of(io.trino.plugin.jdbc.StandardColumnMappings.timeColumnMapping(
                    createTimeType(min(typeHandle.decimalDigits().orElse(3), TimeType.MAX_PRECISION))));
        case Types.TIMESTAMP: {
            // Calcite/Avatica exposes TIMESTAMP via a number-based cursor accessor, which does not
            // support Trino's default read (getObject(LocalDateTime.class)) and fails with
            // "cannot convert to Object". Read the raw epoch milliseconds via getLong instead and
            // scale to the microseconds a short Trino timestamp expects. (Calcite timestamps are
            // millisecond precision, so this is always a short timestamp.)
            int precision = min(typeHandle.decimalDigits().orElse(3), 6);
            TimestampType timestampType = createTimestampType(precision);
            return Optional.of(ColumnMapping.longMapping(
                    timestampType,
                    (resultSet, columnIndex) -> resultSet.getLong(columnIndex) * 1000L,
                    timestampWriteFunction(timestampType)));
        }
        default:
            // Unknown remote type: skip the column. Set unsupported-type-handling to
            // CONVERT_TO_VARCHAR on the catalog to surface it as varchar instead.
            return Optional.empty();
        }
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }
        if (type == TINYINT) {
            return WriteMapping.longMapping("tinyint", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }
        if (type == REAL) {
            return WriteMapping.longMapping("real", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }
        if (type instanceof DecimalType decimalType) {
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }
        if (type instanceof CharType charType) {
            return WriteMapping.sliceMapping("char(" + charType.getLength() + ")", charWriteFunction());
        }
        if (type instanceof VarcharType varcharType) {
            String dataType = varcharType.isUnbounded()
                    ? "varchar"
                    : "varchar(" + varcharType.getBoundedLength() + ")";
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (type == VARBINARY) {
            return WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction());
        }
        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate());
        }
        if (type instanceof TimeType timeType) {
            return WriteMapping.longMapping("time(" + timeType.getPrecision() + ")",
                    timeWriteFunction(timeType.getPrecision()));
        }
        if (type instanceof TimestampType timestampType) {
            String dataType = "timestamp(" + timestampType.getPrecision() + ")";
            if (timestampType.isShort()) {
                return WriteMapping.longMapping(dataType, timestampWriteFunction(timestampType));
            }
            return WriteMapping.objectMapping(dataType,
                    longTimestampWriteFunction(timestampType, timestampType.getPrecision()));
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }
}
