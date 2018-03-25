package com.learn.parse

import scopt.OptionParser

object ParseApp extends App {

  val version = "1.0"

  // https://www.spantree.net/blog/2017/06/26/scala-native-for-cli-tools.html
  // https://github.com/scopt/scopt
  // http://www.learn4master.com/algorithms/spark-program-use-scopt-parse-arguments
  // http://www.marcinkossakowski.com/how-to-create-git-like-command-line-interface-in-scala/ **********


  case class CommandLineArgs(
    environment: String = "",   // Required
    security: String = "",      // Required
    db: String = "",            // Required
    table: String = "",         // Required
    partition: String  = "partition_year_month_day",  // Optional
    numDays: Int = 20)

  val parser = new OptionParser[CommandLineArgs]("parser_app") {
    head("parser app args", ParseApp.version)

    opt[String]('e', "environment").required().valueName("<environment>")
      .action((x, c) => c.copy(environment = x)).text("Setting environment required")

    opt[String]('s', "security").required().valueName("<security>")
      .action((x, c) => c.copy(security = x)).text("Setting security required")

    opt[String]('d', "dbname").required().valueName("<dbname>")
      .action((x, c) => c.copy(db = x)).text("Setting dbname required")

    opt[String]('t', "table").required().valueName("<table>")
      .action((x, c) => c.copy(table = x)).text("Setting table required")

    opt[String]('p', "partition").optional().valueName("<partition>")
      .action((x, c) => c.copy(partition = x)).text("Partition name is optional")

    opt[Int]('n', "numdays").optional().valueName("<numdays>")
      .action( (x, c) => c.copy(numDays = x) ).text("foo is an integer property")

    help("help").text("prints this usage text")

    note("some notes.")
  }

  parser.parse(args, CommandLineArgs()) match {

    case Some(config) =>
      // do stuff
      val environment = config.environment
      val security = config.security
      val dbname = config.db
      val table = config.table
      val partition = config.partition
      val numdays = config.numDays

      // Check that
      //config.mode match {
      //  case "choose" =>
      //    Picker.choose(config.objects, config.count).foreach(println)
      //  case "roll" =>
      //    println(Picker.roll(config.objects(0).toInt, config.objects(1).toInt))
      //}

    case None =>
    // arguments are bad, error message will have been displayed
  }
}
