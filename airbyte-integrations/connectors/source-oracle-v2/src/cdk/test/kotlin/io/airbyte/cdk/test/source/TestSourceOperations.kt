/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.test.source

import io.airbyte.cdk.discover.ArrayFieldType
import io.airbyte.cdk.discover.BigDecimalFieldType
import io.airbyte.cdk.discover.BigIntegerFieldType
import io.airbyte.cdk.discover.BinaryStreamFieldType
import io.airbyte.cdk.discover.BooleanFieldType
import io.airbyte.cdk.discover.ByteFieldType
import io.airbyte.cdk.discover.BytesFieldType
import io.airbyte.cdk.discover.ClobFieldType
import io.airbyte.cdk.discover.DoubleFieldType
import io.airbyte.cdk.discover.FloatFieldType
import io.airbyte.cdk.discover.IntFieldType
import io.airbyte.cdk.discover.LocalDateTimeFieldType
import io.airbyte.cdk.discover.LocalDateFieldType
import io.airbyte.cdk.discover.LocalTimeFieldType
import io.airbyte.cdk.discover.NClobFieldType
import io.airbyte.cdk.discover.NStringFieldType
import io.airbyte.cdk.discover.OffsetDateTimeFieldType
import io.airbyte.cdk.discover.OffsetTimeFieldType
import io.airbyte.cdk.discover.NullFieldType
import io.airbyte.cdk.discover.LosslessFieldType
import io.airbyte.cdk.discover.ShortFieldType
import io.airbyte.cdk.discover.StringFieldType
import io.airbyte.cdk.discover.TableName
import io.airbyte.cdk.discover.UrlFieldType
import io.airbyte.cdk.discover.FieldType
import io.airbyte.cdk.discover.PokemonFieldType
import io.airbyte.cdk.discover.XmlFieldType
import io.airbyte.cdk.jdbc.JdbcMetadataQuerier
import io.airbyte.cdk.read.stream.And
import io.airbyte.cdk.read.stream.Equal
import io.airbyte.cdk.read.stream.From
import io.airbyte.cdk.read.stream.FromNode
import io.airbyte.cdk.read.stream.Greater
import io.airbyte.cdk.read.stream.LesserOrEqual
import io.airbyte.cdk.read.stream.Limit
import io.airbyte.cdk.read.stream.LimitNode
import io.airbyte.cdk.read.stream.LimitZero
import io.airbyte.cdk.read.stream.NoFrom
import io.airbyte.cdk.read.stream.NoLimit
import io.airbyte.cdk.read.stream.NoOrderBy
import io.airbyte.cdk.read.stream.NoWhere
import io.airbyte.cdk.read.stream.Or
import io.airbyte.cdk.read.stream.OrderBy
import io.airbyte.cdk.read.stream.OrderByNode
import io.airbyte.cdk.read.stream.SelectColumnMaxValue
import io.airbyte.cdk.read.stream.SelectColumns
import io.airbyte.cdk.read.stream.SelectNode
import io.airbyte.cdk.read.stream.SelectQuery
import io.airbyte.cdk.read.stream.SelectQueryGenerator
import io.airbyte.cdk.read.stream.SelectQueryRootNode
import io.airbyte.cdk.read.stream.Where
import io.airbyte.cdk.read.stream.WhereClauseLeafNode
import io.airbyte.cdk.read.stream.WhereClauseNode
import io.airbyte.cdk.read.stream.WhereNode
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Secondary
import io.micronaut.context.env.Environment
import jakarta.inject.Singleton
import java.sql.JDBCType

/** Stateless connector-specific logic for [TestSource]. */
@Singleton
@Requires(env = [Environment.TEST])
@Secondary
class TestSourceOperations : JdbcMetadataQuerier.FieldTypeMapper, SelectQueryGenerator {

    override fun toFieldType(c: JdbcMetadataQuerier.ColumnMetadata): FieldType =
        when (c.type.jdbcType) {
            JDBCType.BIT,
            JDBCType.BOOLEAN -> BooleanFieldType
            JDBCType.TINYINT -> ByteFieldType
            JDBCType.SMALLINT -> ShortFieldType
            JDBCType.INTEGER -> IntFieldType
            JDBCType.BIGINT -> BigIntegerFieldType
            JDBCType.FLOAT -> FloatFieldType
            JDBCType.DOUBLE -> DoubleFieldType
            JDBCType.REAL,
            JDBCType.NUMERIC,
            JDBCType.DECIMAL -> BigDecimalFieldType
            JDBCType.CHAR,
            JDBCType.VARCHAR,
            JDBCType.LONGVARCHAR -> StringFieldType
            JDBCType.NCHAR,
            JDBCType.NVARCHAR,
            JDBCType.LONGNVARCHAR -> NStringFieldType
            JDBCType.DATE -> LocalDateFieldType
            JDBCType.TIME -> LocalTimeFieldType
            JDBCType.TIMESTAMP -> LocalDateTimeFieldType
            JDBCType.TIME_WITH_TIMEZONE -> OffsetTimeFieldType
            JDBCType.TIMESTAMP_WITH_TIMEZONE -> OffsetDateTimeFieldType
            JDBCType.BLOB -> BinaryStreamFieldType
            JDBCType.BINARY,
            JDBCType.VARBINARY,
            JDBCType.LONGVARBINARY -> BytesFieldType
            JDBCType.CLOB -> ClobFieldType
            JDBCType.NCLOB -> NClobFieldType
            JDBCType.DATALINK -> UrlFieldType
            JDBCType.SQLXML -> XmlFieldType
            JDBCType.ARRAY -> ArrayFieldType(StringFieldType)
            JDBCType.NULL -> NullFieldType
            JDBCType.OTHER,
            JDBCType.JAVA_OBJECT,
            JDBCType.DISTINCT,
            JDBCType.STRUCT,
            JDBCType.REF,
            JDBCType.ROWID,
            JDBCType.REF_CURSOR,
            null -> PokemonFieldType
        }

    private fun TableName.fullyQualifiedName(): String =
        if (schema == null) name else "${schema}.${name}"

    override fun generate(ast: SelectQueryRootNode): SelectQuery =
        SelectQuery(ast.sql(), ast.select.columns, ast.bindings())

    fun SelectQueryRootNode.sql(): String {
        val components: List<String> =
            listOf(select.sql(), from.sql(), where.sql(), orderBy.sql(), limit.sql())
        return components.filter { it.isNotBlank() }.joinToString(" ")
    }

    fun SelectNode.sql(): String =
        when (this) {
            is SelectColumns -> "SELECT " + columns.map { it.id }.joinToString(", ")
            is SelectColumnMaxValue -> "SELECT MAX(${column.id})"
        }

    fun FromNode.sql(): String =
        when (this) {
            NoFrom -> ""
            is From -> {
                val fullyQualifiedName: String =
                    if (table.schema == null) {
                        table.name
                    } else {
                        "${table.schema}.${table.name}"
                    }
                "FROM $fullyQualifiedName"
            }
        }

    fun WhereNode.sql(): String =
        when (this) {
            NoWhere -> ""
            is Where -> "WHERE ${clause.sql()}"
        }

    fun WhereClauseNode.sql(): String =
        when (this) {
            is And -> conj.map { it.sql() }.joinToString(") AND (", "(", ")")
            is Or -> disj.map { it.sql() }.joinToString(") OR (", "(", ")")
            is Equal -> "${column.id} = ?"
            is Greater -> "${column.id} > ?"
            is LesserOrEqual -> "${column.id} <= ?"
        }

    fun OrderByNode.sql(): String =
        when (this) {
            NoOrderBy -> ""
            is OrderBy -> "ORDER BY " + columns.map { it.id }.joinToString(", ")
        }

    fun LimitNode.sql(): String =
        when (this) {
            NoLimit -> ""
            LimitZero -> "LIMIT 0"
            is Limit -> "LIMIT ${state.current}"
        }

    fun SelectQueryRootNode.bindings(): List<SelectQuery.Binding> = where.bindings()

    fun WhereNode.bindings(): List<SelectQuery.Binding> =
        when (this) {
            is NoWhere -> listOf()
            is Where -> clause.bindings()
        }

    fun WhereClauseNode.bindings(): List<SelectQuery.Binding> =
        when (this) {
            is And -> conj.flatMap { it.bindings() }
            is Or -> disj.flatMap { it.bindings() }
            is WhereClauseLeafNode -> {
                val type = column.type as LosslessFieldType
                listOf(SelectQuery.Binding(bindingValue, type))
            }
        }
}
