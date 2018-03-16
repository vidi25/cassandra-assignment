package edu.knoldus

import org.apache.log4j.Logger

object MainApplication extends App{

  val log = Logger.getLogger(this.getClass)
   val operations = new Operations
  log.info(s"\n\n${operations.createKeyspace}\n\n")
  log.info(s"${operations.createTable}\n\n")
  log.info(s"${operations.insertData}\n\n")
  log.info(s"${operations.createTableByCity}\n\n")
  log.info(s"${operations.insertDataByCity}\n\n")
  log.info(s"${operations.createIndexByCity}\n\n")
  log.info(s"Data fetched by id = \n ${operations.fetchDataOfAnId}\n\n")
  log.info(s"Data fetched after updating = \n ${operations.updateRecord}\n\n")
  log.info(s"Data fetched by salary and id = \n ${operations.fetchDataFromEmployeeBySalaryAndId}\n\n")
  log.info(s"Data fetched by city = \n ${operations.fetchDataByCity}\n\n")
  log.info(s"Data fetched after deleting = \n ${operations.deleteDataByCity}\n\n")

}
