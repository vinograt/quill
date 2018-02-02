package io.getquill

import com.github.mauricio.async.db.{ QueryResult ⇒ DBQueryResult }
import com.github.mauricio.async.db.pool.{ ConnectionPoolListener, PartitionedConnectionPool }
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.typesafe.config.Config
import io.getquill.context.async.{ ArrayDecoders, ArrayEncoders, AsyncContext, UUIDObjectEncoding }
import io.getquill.util.LoadConfig

class PostgresAsyncContext[N <: NamingStrategy](pool: PartitionedConnectionPool[PostgreSQLConnection])
  extends AsyncContext[PostgresDialect, N, PostgreSQLConnection](pool)
  with ArrayEncoders
  with ArrayDecoders
  with UUIDObjectEncoding {

  def this(config: PostgresAsyncContextConfig) = this(config.pool)
  def this(config: Config, poolListener: ConnectionPoolListener) = this(PostgresAsyncContextConfig(config, Some(poolListener)))
  def this(config: Config) = this(PostgresAsyncContextConfig(config))
  def this(configPrefix: String) = this(LoadConfig(configPrefix))

  override protected def extractActionResult[O](returningColumn: String, returningExtractor: Extractor[O])(result: DBQueryResult): O =
    returningExtractor(result.rows.get(0))

  override protected def expandAction(sql: String, returningColumn: String): String =
    s"$sql RETURNING $returningColumn"
}
