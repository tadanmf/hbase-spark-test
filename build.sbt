organizationName := "io.datadynamics"
name := "hbase-spark"
version := "0.1"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "junit" % "junit" % "4.13.2" % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.3" % Test,

  // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
  "org.apache.hadoop" % "hadoop-common" % "3.1.1.3.0.1.0-187",
  "org.apache.hadoop" % "hadoop-hdfs" % "3.1.1.3.0.1.0-187",
  "org.apache.hadoop" % "hadoop-client" % "3.1.1.3.0.1.0-187",

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
  "org.apache.spark" %% "spark-sql" % "2.3.1.3.0.1.0-187",

// https://mvnrepository.com/artifact/org.apache.hbase/hbase-common
  "org.apache.hbase" % "hbase-common" % "2.0.0.3.0.1.0-187",
  "org.apache.hbase" % "hbase-client" % "2.0.0.3.0.1.0-187",
  "org.apache.hbase" % "hbase-server" % "2.0.0.3.0.1.0-187",
  "org.apache.hbase" % "hbase-mapreduce" % "2.0.0.3.0.1.0-187",
  "org.apache.hbase" % "hbase-hadoop-compat" % "2.0.0.3.0.1.0-187",
  "org.apache.hbase" % "hbase-hadoop2-compat" % "2.0.0.3.0.1.0-187",
  "org.apache.hbase" % "hbase-zookeeper" % "2.0.0.3.0.1.0-187",

  // https://mvnrepository.com/artifact/org.apache.commons/commons-compress
  "org.apache.commons" % "commons-compress" % "1.19",

  // https://mvnrepository.com/artifact/com.typesafe/config
  "com.typesafe" % "config" % "1.4.1"
)

mainClass in assembly := Some("io.datadynamics.HbaseSpark")

assemblyJarName in assembly := "spark_hbase_test.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// scala 라이브러리를 제외하고 싶은 경우 추가
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

resolvers += "Hortonworks Repository" at "https://repo.hortonworks.com/content/repositories/releases"
resolvers += Resolver.mavenLocal

logLevel := Level.Error
logLevel in assembly := Level.Error

import deployssh.DeploySSH.ServerConfig
import fr.janalyse.ssh.{SSH, SSHCommand}

lazy val myProject = project.enablePlugins(DeploySSH).settings(
  deployConfigs ++= Seq(
    ServerConfig("tt05gn001.hdp.local", "172.30.1.242", user = Some("root"), sshDir = Some("."), sshKeyFile = Some("id_rsa_root_tt05cn001"))
  ),
  deploySshExecAfter ++= Seq(
    (ssh: SSH) => {
      ssh.scp(scp => {
        scp.send(file(s"./target/scala-2.11/spark_hbase_test.jar"), s"/root/Downloads/spark_hbase_test.jar")
      })
      ssh.execute(SSHCommand("ls -alF /root/spark_hbase_test.jar"))
    }
  )
)