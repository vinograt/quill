package io.getquill

import com.github.mauricio.async.db.pool.ConnectionPoolListener
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.postgresql.pool.PostgreSQLConnectionFactory
import com.github.mauricio.async.db.postgresql.util.URLParser
import com.typesafe.config.Config
import io.getquill.context.async.AsyncContextConfig

case class PostgresAsyncContextConfig(config: Config, poolListener: Option[ConnectionPoolListener] = None)
  extends AsyncContextConfig[PostgreSQLConnection](config, new PostgreSQLConnectionFactory(_), URLParser, poolListener)
