package com.arrow.sql.types

import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.mutable.ListBuffer

object StructTypeHelpers {
  def getColumns(
      dt: DataType,
      path: String = "",
      listOfColumns: ListBuffer[String] = ListBuffer[String]()
  ): ListBuffer[String] = {
    dt match {
      case s: StructType =>
        s.fields.foreach(f =>
          if (f.dataType.isInstanceOf[StructType] || !f.name.contains(".")) {
            getColumns(f.dataType, path + "." + f.name, listOfColumns)
          } else {
            getColumns(f.dataType, path + ".`" + f.name + "`", listOfColumns)
          }
        )
      case _ => listOfColumns += path
    }
    listOfColumns.map(colName => colName.substring(1))
  }
}
