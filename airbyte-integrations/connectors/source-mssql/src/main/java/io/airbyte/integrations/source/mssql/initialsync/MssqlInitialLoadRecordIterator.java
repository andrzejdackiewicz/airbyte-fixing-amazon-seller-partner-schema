package io.airbyte.integrations.source.mssql.initialsync;

import static io.airbyte.cdk.integrations.source.relationaldb.RelationalDbQueryUtils.enquoteIdentifier;
import static io.airbyte.cdk.integrations.source.relationaldb.RelationalDbQueryUtils.getFullyQualifiedTableNameWithQuoting;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.AbstractIterator;
import io.airbyte.cdk.db.JdbcCompatibleSourceOperations;
import io.airbyte.cdk.db.jdbc.JdbcDatabase;
import io.airbyte.cdk.integrations.source.relationaldb.RelationalDbQueryUtils;
import io.airbyte.cdk.integrations.source.relationaldb.models.OrderedColumnLoadStatus;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.source.mssql.initialsync.MssqlInitialReadUtil.OrderedColumnInfo;
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.CheckForNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("try")
public class MssqlInitialLoadRecordIterator extends AbstractIterator<JsonNode>
    implements AutoCloseableIterator<JsonNode> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MssqlInitialLoadRecordIterator.class);

  private AutoCloseableIterator<JsonNode> currentIterator;
  private final JdbcDatabase database;
  private int numSubqueries = 0;
  private final String quoteString;
  private final JdbcCompatibleSourceOperations<JDBCType> sourceOperations;
  private final List<String> columnNames;
  private final AirbyteStreamNameNamespacePair pair;
  private final MssqlInitialLoadStateManager initialLoadStateManager;
  private final long chunkSize;
  private final OrderedColumnInfo ocInfo;
  private final boolean isCompositeKeyLoad;

  MssqlInitialLoadRecordIterator(
      final JdbcDatabase database,
      final JdbcCompatibleSourceOperations<JDBCType> sourceOperations,
      final String quoteString,
      final MssqlInitialLoadStateManager initialLoadStateManager,
      final List<String> columnNames,
      final AirbyteStreamNameNamespacePair pair,
      final long chunkSize,
      final boolean isCompositeKeyLoad) {
    this.database = database;
    this.sourceOperations = sourceOperations;
    this.quoteString = quoteString;
    this.initialLoadStateManager = initialLoadStateManager;
    this.columnNames = columnNames;
    this.pair = pair;
    this.chunkSize = chunkSize;
    this.ocInfo = initialLoadStateManager.getOrderedColumnInfo(pair);
    this.isCompositeKeyLoad = isCompositeKeyLoad;
  }
  @CheckForNull
  @Override
  protected JsonNode computeNext() {
    if (shouldBuildNextSubquery()) {
      try {
        // We will only issue one query for a composite key load. If we have already processed all the data
        // associated with this
        // query, we should indicate that we are done processing for the given stream.
        if (isCompositeKeyLoad && numSubqueries >= 1) {
          return endOfData();
        }
        // Previous stream (and connection) must be manually closed in this iterator.
        if (currentIterator != null) {
          currentIterator.close();
        }

        LOGGER.info("Subquery number : {}", numSubqueries);
        final Stream<JsonNode> stream = database.unsafeQuery(
            this::getPkPreparedStatement, sourceOperations::rowToJson);
        currentIterator = AutoCloseableIterators.fromStream(stream, pair);
        numSubqueries++;
        // TODO: check if a subquery can be dry
        // If the current subquery has no records associated with it, the entire stream has been read.
        if (!currentIterator.hasNext()) {
          return endOfData();
        }
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }
    return currentIterator.next();
  }

  private boolean shouldBuildNextSubquery() {
    // The next sub-query should be built if (i) it is the first subquery in the sequence. (ii) the previous subquery has finished.
    return (currentIterator == null || !currentIterator.hasNext());
  }

  private PreparedStatement getPkPreparedStatement(final Connection connection) {
    try {
      final String tableName = pair.getName();
      final String schemaName = pair.getNamespace();
      final String fullTableName = getFullyQualifiedTableNameWithQuoting(schemaName, tableName,
          quoteString);
      LOGGER.info("Preparing query for table: {}", fullTableName);
      final String wrappedColumnNames = RelationalDbQueryUtils.enquoteIdentifierList(columnNames, quoteString);
      final OrderedColumnLoadStatus ocLoadStatus = initialLoadStateManager.getOrderedColumnLoadStatus(pair);
      if (ocLoadStatus == null) {
        final String quotedCursorField = enquoteIdentifier(ocInfo.ocFieldName(), quoteString); // TODO: check quoting
        final String sql;
        // TODO: deal with composite ordered column
        if (isCompositeKeyLoad) {
          sql = "SELECT %s FROM %s ORDER BY %s".formatted(wrappedColumnNames, fullTableName, quotedCursorField);
        } else {
          sql = "SELECT %s FROM %s ORDER BY %s LIMIT %s".formatted(wrappedColumnNames, fullTableName, quotedCursorField, chunkSize);
        }
        final PreparedStatement preparedStatement = connection.prepareStatement(sql);
        LOGGER.info("Executing query for table {}: {}", tableName, preparedStatement);
        return preparedStatement;
      } else {
        LOGGER.info("ocLoadStatus value is : {}", ocLoadStatus.getOrderedColVal());
        final String quotedCursorField = enquoteIdentifier(ocInfo.ocFieldName(), quoteString); // TODO: check quoting
        final String sql;
        if (isCompositeKeyLoad) {
          sql = "SELECT %s FROM %s WHERE %s >= ? ORDER BY %s".formatted(wrappedColumnNames, fullTableName,
              quotedCursorField, quotedCursorField);
        } else {
          // The ordered column max value could be null - this can happen in the case of empty tables. In this case, we
          // can just issue a query
          // without any chunking.
          if (ocInfo.ocMaxValue() != null) {
            sql = "SELECT %s FROM %s WHERE %s > ? AND %s <= ? ORDER BY %s LIMIT %s".formatted(wrappedColumnNames, fullTableName,
                quotedCursorField, quotedCursorField, quotedCursorField, chunkSize);
          } else {
            sql = "SELECT %s FROM %s WHERE %s > ? ORDER BY %s".formatted(wrappedColumnNames, fullTableName,
                quotedCursorField, quotedCursorField);
          }
        }
        final PreparedStatement preparedStatement = connection.prepareStatement(sql);
        final JDBCType cursorFieldType = ocInfo.fieldType();
        sourceOperations.setCursorField(preparedStatement, 1, cursorFieldType, ocLoadStatus.getOrderedColVal());
        if (!isCompositeKeyLoad && ocInfo.ocMaxValue() != null) {
          sourceOperations.setCursorField(preparedStatement, 2, cursorFieldType, ocInfo.ocMaxValue()); // TODO: check here
        }
        LOGGER.info("Executing query for table {}: {}", tableName, preparedStatement);
        return preparedStatement;
      }
    } catch (final SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    if (currentIterator != null) {
      currentIterator.close();
    }
  }
}
