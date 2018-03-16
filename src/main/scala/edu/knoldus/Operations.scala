package edu.knoldus

import com.datastax.driver.core.{ConsistencyLevel, Row, Session}
import edu.knoldus.CassandraProvider._

import scala.collection.JavaConverters._

class Operations extends {


  val cassandraSession: Session = getCassandraSession

  cassandraSession.getCluster.getConfiguration.getQueryOptions.setConsistencyLevel(ConsistencyLevel.QUORUM)

  /**
    * creates a keyspace.
    * @return String message that keyspace is created
    */
  def createKeyspace: String = {
    cassandraSession.execute(s"CREATE KEYSPACE IF NOT EXISTS  $cassandraKeyspace WITH REPLICATION = " +
      s"{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    cassandraSession.execute(s"USE $cassandraKeyspace")
    s"keyspace created"
  }

  /**
    * creates Employee table with emp_id as partition key and emp_salary as clustering column.
    * @return String message that table is created
    */
  def createTable: String = {
    val createTableQuery = "CREATE TABLE IF NOT EXISTS Employee" +
      "(emp_id int, emp_name text,emp_city text,emp_salary varint" +
      ",emp_phone varint,PRIMARY KEY(emp_id,emp_salary))"
    cassandraSession.execute(createTableQuery)
    s"Employee table created"
  }

  /**
    * inserts data in Employee table.
    * @return String message that data is inserted in Employee table
    */
  def insertData: String = {
    val insertQuery1 = "INSERT INTO Employee(emp_id, emp_name,emp_city ,emp_salary ,emp_phone) VALUES(1,'Ajay','Delhi',50000,9905432112) IF NOT EXISTS "
    val insertQuery2 = "INSERT INTO Employee(emp_id, emp_name,emp_city ,emp_salary ,emp_phone) VALUES(2,'Aman','Lucknow',70000,987654321) IF NOT EXISTS"
    val insertQuery3 = "INSERT INTO Employee(emp_id, emp_name,emp_city ,emp_salary ,emp_phone) VALUES(3,'Megha','Chandigarh',60000,9871234760) IF NOT EXISTS"
    val insertQuery4 = "INSERT INTO Employee(emp_id, emp_name,emp_city ,emp_salary ,emp_phone) VALUES(4,'Suman','Mumbai',20000,9876500651) IF NOT EXISTS"
    cassandraSession.execute(insertQuery1)
    cassandraSession.execute(insertQuery2)
    cassandraSession.execute(insertQuery3)
    cassandraSession.execute(insertQuery4)
    s"data inserted in Employee table"
  }

  /**
    * creates EmployeeByCity table where city is the primary key.
    * @return String message that table is created
    */
  def createTableByCity: String = {
    val createTableQuery = "CREATE TABLE IF NOT EXISTS EmployeeByCity" +
      "(emp_id int, emp_name text,emp_city text PRIMARY KEY,emp_salary varint" +
      ",emp_phone varint)"
    cassandraSession.execute(createTableQuery)
    s"EmployeeByCity table created"
  }

  /**
    * inserts data in EmployeeByCity table.
    * @return String that data has been inserted.
    */
  def insertDataByCity: String = {
    val insertQuery1 = "INSERT INTO EmployeeByCity(emp_id, emp_name,emp_city ,emp_salary ,emp_phone) VALUES(1,'Ajay','Delhi',50000,9905432112) IF NOT EXISTS "
    val insertQuery2 = "INSERT INTO EmployeeByCity(emp_id, emp_name,emp_city ,emp_salary ,emp_phone) VALUES(2,'Aman','Lucknow',70000,987654321) IF NOT EXISTS"
    val insertQuery3 = "INSERT INTO EmployeeByCity(emp_id, emp_name,emp_city ,emp_salary ,emp_phone) VALUES(3,'Megha','Chandigarh',60000,9871234760) IF NOT EXISTS"
    val insertQuery4 = "INSERT INTO EmployeeByCity(emp_id, emp_name,emp_city ,emp_salary ,emp_phone) VALUES(4,'Suman','Mumbai',20000,9876500651) IF NOT EXISTS"
    cassandraSession.execute(insertQuery1)
    cassandraSession.execute(insertQuery2)
    cassandraSession.execute(insertQuery3)
    cassandraSession.execute(insertQuery4)
    s"data inserted in EmployeeByCity table"
  }

  /**
    * creates an index on emp_city column.
    * @return String message that index has been created
    */
  def createIndexByCity: String = {
    val createIndexQuery = "CREATE INDEX IF NOT EXISTS EmployeeCityIndex ON Employee(emp_city)"
    cassandraSession.execute(createIndexQuery)
    s"EmployeeSalaryIndex index created"
  }

  /**
    * returns employee data of a particular id.
    * @return List of resultant data
    */
  def fetchDataOfAnId: List[Row] = {
    val fetchDataForIdQuery = "SELECT * FROM Employee WHERE emp_id = 2"
    cassandraSession.execute(fetchDataForIdQuery).asScala.toList
  }

  /**
    * updates the employee city where id = 1 and salary=50000.
    * @return List of updated data
    */
  def updateRecord: List[Row] = {
    val updateFourthRecordQuery = "UPDATE Employee SET emp_city='Chandigarh' WHERE emp_id = 1 and emp_salary=50000"
    cassandraSession.execute(updateFourthRecordQuery)
    cassandraSession.execute("SELECT * FROM Employee").asScala.toList
  }

  /**
    * returns employee data whose id = 1 and salary > 30000.
    * @return List of fetched data
    */
  def fetchDataFromEmployeeBySalaryAndId: List[Row] = {
    val fetchDataFromIndexQuery = "SELECT * FROM employee WHERE emp_salary > 30000 and emp_id = 1"
    cassandraSession.execute(fetchDataFromIndexQuery).asScala.toList
  }

  /**
    * returns employee data whose city = Chandigarh.
    * @return List of fetched data
    */
  def fetchDataByCity: List[Row] = {
    val fetchDataByCityQuery = "SELECT * FROM Employee where emp_city='Chandigarh'"
    cassandraSession.execute(fetchDataByCityQuery).asScala.toList
  }

  /**
    * returns deleted employee data whose city = Chandigarh
    * @return List of updated data
    */
  def deleteDataByCity: List[Row] = {
    val deleteDataQuery = "DELETE FROM EmployeeByCity where emp_city='Chandigarh'"
    cassandraSession.execute(deleteDataQuery)
    cassandraSession.execute("SELECT * FROM EmployeeByCity").asScala.toList
  }

}
