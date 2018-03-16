package edu.knoldus

import com.datastax.driver.core.{Cluster, Session}
import com.typesafe.config.{Config, ConfigFactory}

object CassandraProvider {

  val config: Config = ConfigFactory.load()
  val cassandraKeyspace: String = config.getString("cassandra.keyspace")
  val cassandraHostname: String = config.getString("cassandra.contact.points")

  def getCassandraSession: Session = {

      val cluster = new Cluster.Builder().withClusterName("Test Cluster").
        addContactPoints(cassandraHostname).build
      cluster.connect

  }

}
