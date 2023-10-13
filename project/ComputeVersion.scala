import scala.sys.process._
import scala.util.matching.Regex

object ComputeVersion {

  lazy val version: String = scala.util.Properties.envOrElse("VERSION", "0.0-local")

}
