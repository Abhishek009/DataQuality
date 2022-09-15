package com.data.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object ColumnExt {
  
    implicit class ColumnMethods(col: Column) {

   

    def isNotIn(list: Any*): Column = {
      not(col.isin(list: _*))

    }
    
        /**
     * Returns true if the col is not null or a blank string
     *
     * The `isNotNullOrBlank` method returns `true` if a column is not `null` or `blank` and `false` otherwise.
     * Suppose you start with the following `sourceDF`:
     *
     * +-------------+
     * |employee_name|
     * +-------------+
     * |         John|
     * |         null|
     * |           ""|
     * |       "    "|
     * +-------------+
     *
     * Run the `isNotNullOrBlank` method:
     *
     * {{{
     * val actualDF = sourceDF.withColumn(
     *   "employee_name_is_not_null_or_blank",
     *   col("employee_name").isNotNullOrBlank
     * )
     * }}}
     *
     * Here are the contents of `actualDF`:
     *
     * +-------------+----------------------------------+
     * |employee_name|employee_name_is_not_null_or_blank|
     * +-------------+----------------------------------+
     * |         John|                              true|
     * |         null|                             false|
     * |           ""|                             false|
     * |       "    "|                             false|
     * +-------------+----------------------------------+
     *
     * @group expr_ops
     */
    def isNotNullOrBlank: Column = {
      !col.isNullOrBlank
    }
    
      /**
     * Returns true if the col is null or a blank string
     * The `isNullOrBlank` method returns `true` if a column is `null` or `blank` and `false` otherwise.
     *
     * Suppose you start with the following `sourceDF`:
     *
     * {{{
     * +-----------+
     * |animal_type|
     * +-----------+
     * |        dog|
     * |       null|
     * |         ""|
     * |     "    "|
     * +-----------+
     * }}}
     *
     * Run the `isNullOrBlank` method:
     *
     * {{{
     * val actualDF = sourceDF.withColumn(
     * "animal_type_is_null_or_blank",
     * col("animal_type").isNullOrBlank
     * )
     * }}}
     *
     * Here are the contents of `actualDF`:
     *
     * {{{
     * +-----------+----------------------------+
     * |animal_type|animal_type_is_null_or_blank|
     * +-----------+----------------------------+
     * |        dog|                       false|
     * |       null|                        true|
     * |         ""|                        true|
     * |     "    "|                        true|
     * +-----------+----------------------------+
     * }}}
     *
     * @group expr_ops
     */
    def isNullOrBlank: Column = {
      col.isNull || trim(col) === ""
    }

  }
}