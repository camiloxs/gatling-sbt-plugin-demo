
import io.gatling.core.Predef.{scenario, _}
import io.gatling.http.Predef._

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.{Path, Paths}
import scala.collection.mutable._

class Prices extends Simulation {

  private val httpProtocol = http.baseUrl("https://finance.yahoo.com")
    .acceptHeader("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8") // Here are the common headers
    .acceptEncodingHeader("gzip, deflate")
    .acceptLanguageHeader("en-US,en;q=0.5")
    .userAgentHeader("Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:16.0) Gecko/20100101 Firefox/16.0")
    .shareConnections

  private val urls = csv("prices_urls.csv")

  val outputPath: String = System.getProperty("outputPath", "output.csv")
  val outputFile: Path = Paths.get(outputPath)
  val outputWriter: BufferedWriter = new BufferedWriter(new FileWriter(outputFile.toFile))

  private val priceTable: ArrayBuffer[Map[String, Any]] = new ArrayBuffer()
  private val inputDateFormat = new java.text.SimpleDateFormat("MMM dd, yyyy")
  private val outputDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")

  private val scn = scenario("Look for last price")
    .feed(urls)
    .exec(http("Load Page")
      .get("/#{url}")
      .check(css("table > tbody")
        .ofType[Node]
        .saveAs("table")))
    .exitHereIfFailed
    .exec { session =>
      val table = session("table").as[Node]
      val order = session("order").as[Int]
      val symbol = session("symbol").as[String]

      table.getChildElements.foreach { tableRow =>
        val date = tableRow.getChildElement(0).getTextContent
        val lastPrice = tableRow.getChildElement(4).getTextContent
          .replace(",", "")

        //println(s"»»»»$date | $symbol")
        val row = Map(
          "date" -> date,
          "symbol" -> symbol,
          "price" -> lastPrice,
          "order" -> order
        )
        synchronized {
          priceTable.addOne(row)
        }
      }
      session
    }

  setUp(scn.inject(atOnceUsers(29)).protocols(httpProtocol))

  after {
    //println("Simulation is finished!!!!!")
    priceTable
      .groupBy(_ ("date"))
      .map { case (dateStr, table) =>
        val values = table
          .sortBy(_ ("order").asInstanceOf[Int])
          .map(_ ("price"))
        val date = inputDateFormat.parse(dateStr.toString)
        val formattedDate = outputDateFormat.format(date)
        date :: formattedDate :: values.toList
      }
      .toList
      .sortBy(_.head.asInstanceOf[java.util.Date])
      .map(_.tail)
      .foreach { row =>
        outputWriter.write(s"${row.mkString(",")}\n")
      }

    outputWriter.flush()
    outputWriter.close()
  }
}
