package org.apache.spark.sql.execution.datasources.v2.wasm

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class WasmDataSourceV2 extends FileDataSourceV2 {
  /**
   * Returns a V1 [[FileFormat]] class of the same file data source.
   * This is a solution for the following cases:
   * 1. File datasource V2 implementations cause regression. Users can disable the problematic data
   * source via SQL configuration and fall back to FileFormat.
   * 2. Catalog support is required, which is still under development for data source V2.
   */
  override def fallbackFileFormat: Class[_ <: FileFormat] = ???

  override protected def getTable(options: CaseInsensitiveStringMap): Table = ???

  /**
   * The string that represents the format that this data source provider uses. This is
   * overridden by children to provide a nice alias for the data source. For example:
   *
   * {{{
   *   override def shortName(): String = "parquet"
   * }}}
   *
   * @since 1.5.0
   */
  override def shortName(): String = ???
}
