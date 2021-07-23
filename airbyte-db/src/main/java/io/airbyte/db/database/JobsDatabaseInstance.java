package io.airbyte.db.database;

import static org.jooq.impl.DSL.select;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.db.Database;
import io.airbyte.db.Databases;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.db.ServerUuid;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobsDatabaseInstance implements DatabaseInstance {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobsDatabaseInstance.class);
  private static final Function<Database, Boolean> IS_JOBS_DATABASE_CONNECTED = database -> {
    try {
      LOGGER.info("Testing jobs database connection...");
      return database.query(ctx -> ctx.fetchExists(select().from("information_schema.tables")));
    } catch (Exception e) {
      return false;
    }
  };
  public static final Function<Database, Boolean> IS_JOBS_DATABASE_READY = database -> {
    try {
      LOGGER.info("Testing if jobs database is ready...");
      Optional<String> uuid = ServerUuid.get(database);
      return uuid.isPresent();
    } catch (Exception e) {
      return false;
    }
  };

  private final String username;
  private final String password;
  private final String connectionString;
  private final String schema;

  /**
   * @param connectionString in the format of jdbc:postgresql://${DATABASE_HOST}:${DATABASE_PORT/${DATABASE_DB}
   */
  @VisibleForTesting
  public JobsDatabaseInstance(String username, String password, String connectionString, String schema) {
    this.username = username;
    this.password = password;
    this.connectionString = connectionString;
    this.schema = schema;
  }

  public JobsDatabaseInstance(String username, String password, String connectionString) throws IOException {
    this.username = username;
    this.password = password;
    this.connectionString = connectionString;
    this.schema = MoreResources.readResource("job_tables/schema.sql");
  }

  @Override
  public Database get() {
    // When we don't need to setup the database, it means the database is initialized
    // somewhere else, and it is considered ready only when data has been loaded into it.
    return Databases.createPostgresDatabaseWithRetry(
        username,
        password,
        connectionString,
        IS_JOBS_DATABASE_READY);
  }

  /**
   *
   */
  @Override
  public Database getAndInitialize() throws IOException {
    // When we need to setup the database, it means the database will be initialized after
    // we connect to the database. So the database itself is considered ready as long as
    // the connection is alive.
    Database database = Databases.createPostgresDatabaseWithRetry(
        username,
        password,
        connectionString,
        IS_JOBS_DATABASE_CONNECTED);

    new ExceptionWrappingDatabase(database).transaction(ctx -> {
      boolean hasTables = DatabaseInstance.hasTable(ctx, "airbyte_metadata") &&
          DatabaseInstance.hasTable(ctx, "jobs") &&
          DatabaseInstance.hasTable(ctx, "attempts");
      if (hasTables) {
        return null;
      }
      LOGGER.info("Jobs database has not been initialized; initializing tables with schema: {}", schema);
      ctx.execute(schema);
      return null;
    });

    return database;
  }

}
