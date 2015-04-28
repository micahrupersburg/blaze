package gdg.blaze

import org.apache.commons.lang3.StringEscapeUtils
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation.SingleValue

import scala.io.Source
import scala.util.parsing.combinator.{RegexParsers, JavaTokenParsers}


class ConfigParser extends JavaTokenParsers {
  //  override protected val whiteSpace = """(\s|#.*?\n)+""".r

  def config(expression: String) = parseAll(top, expression)

  def input: Parser[List[Body]] = "input" ~> block

  def filter: Parser[List[Body]] = "filter" ~> block

  def output: Parser[List[Body]] = "output" ~> block

  def top: Parser[EntireConfig] = opt(input) ~ opt(filter) ~ opt(output) ^^ {
    case i ~ f ~ o => new EntireConfig(i.getOrElse(List.empty), f.getOrElse(List.empty), o.getOrElse(List.empty))
  }

  def booleanLiteral: Parser[Boolean] = ("true" | "false") ^^ (_.toBoolean)

  def parseNum(str: String): SingleFloat = {
    new SingleFloat(str.toDouble)
  }

  def singular: Parser[Singular] = (str ^^ (new SingleString(_))) | floatingPointNumber ^^ parseNum | (booleanLiteral ^^ (new SingleBool(_))) //| "null"
  def fake : Parser[Any] = repsep(value, ",")
  def value: Parser[Value] = arr | singular | obj | namedObj

  val member: Parser[Member] = (ident | str) ~ "=>" ~ value ^^ { case l ~ "=>" ~ r => new Member(l, r) }

  def namedObj: Parser[NamedObjectValue] = ident ~ opt(obj) ^^ { case l ~ r => new NamedObjectValue(l, r.getOrElse(new ObjectValue(List[Member]())).value) }

  def obj: Parser[ObjectValue] = "{" ~> rep(member) <~ "}" ^^ (new ObjectValue(_))

  def arr: Parser[ArrayValue] = "[" ~> repsep(value, ",") <~ "]" ^^ (new ArrayValue(_))

  def path: Parser[Path] = ((ident ^^ (List(_))) | rep1("[" ~> ident <~ "]")) ^^ (new Path(_))

  def block: Parser[List[Body]] = "{" ~> rep(ifOrOb) <~ "}"


  def ifClause: Parser[IfCond] = "if" ~> cond ~ block ~ rep(elseIfClause) ~ opt(elseClause) ^^ {

    case c ~ b ~ ei ~ e  => new IfCond(c, b, ei ++ e)
  }

  def elseClause: Parser[Else] = "else" ~> block ^^ (new Else(_))

  def elseIfClause: Parser[IfCond] = "else" ~> ifClause

  def ifOrOb: Parser[Body] = ifClause | namedObj

  def equality: Parser[Compare] = ("==" | "!=" | "<" | "<=" | ">" | ">=") ^^ {
    case "==" => EqOp
    case "!=" => NeOp
    case "<" => LtOp
    case "<=" => LteOp
    case ">" => GtOp
    case ">=" => GteOp
  }

  def pathOrSingle: Parser[PathOrSingle] = path | singular

  //  Copied from http://stackoverflow.com/questions/172303/is-there-a-regular-expression-to-detect-a-valid-regular-expression
  def regexLiteral: Parser[String] = "/^((?:(?:[^?+*{}()[\\]\\\\|]+|\\\\.|\\[(?:\\^?\\\\.|\\^[^\\\\]|[^\\\\^])(?:[^\\]\\\\]+|\\\\.)*\\]|\\((?:\\?[:=!]|\\?<[=!]|\\?>)?(?1)??\\)|\\(\\?(?:R|[+-]?\\d+)\\))(?:(?:[?+*]|\\{\\d+(?:,\\d*)?\\})[?+]?)?|\\|)*)$/"

  def singleRegex: Parser[Condition] = pathOrSingle ~ ("=~" | "!~") ~ reVal ^^ {
    case l ~ "=~" ~ r => new ReCompare(l, r)
    case l ~ "!~" ~ r=> new NegativeCondition(new ReCompare(l,r))
  }

  def singleIn: Parser[Condition] = pathOrSingle ~ (opt("not") <~ "in") ~ (pathOrSingle | arr) ^^ {
    case l ~ Some("not") ~ r => new NotInOp(l, r)
    case l ~ None ~ r => new InOp(l, r)
  }

  def singleEq: Parser[Condition] = pathOrSingle ~ equality ~ pathOrSingle ^^ {
    case l ~ e ~ r => new Comparison(l, e, r)
  }

  def reVal: Parser[ReVal] = (str ^^ (new ReString(_))) | (regexLiteral ^^ (new ReLiteral(_)))

  def singleCondition: Parser[Condition] = singleRegex | singleEq | singleIn | (path ^^ (new BooleanPath(_)))

  def optParen(parser: Parser[Any]): Parser[Any] = ("(" ~> parser <~ ")") | parser

  def boolopt: Parser[BooleanOp] = ("and" | "or" | "nand" | "xor") ^^ {
    case "and" => AndOp
    case "or" => OrOp
    case "nand" => NandOp
    case "xor" => XorOp
  }

  def compound: Parser[Condition] = (singleCondition ~ opt(boolopt ~ cond)) ^^ {
    case l ~ r if r.isEmpty => l
    case l ~ r =>
      new CompoundCondition(l, new PredicateCondition(r.get._1, r.get._2))
  }

  def cond: Parser[Condition] = (opt("!") ~ ("(" ~> compound <~ ")" | compound)) ^^ {
    case l ~ r if l.isEmpty => r
    case l ~ r => new NegativeCondition(r)
  }

  def str: Parser[String] =   ("\""+"""([^"\p{Cntrl}\\]|\\([\\'"bfnrt]?)|\\u[a-fA-F0-9]{4})*"""+"\"").r ^^ { x=>
    StringEscapeUtils.escapeJava(x.substring(1, x.length-1))
  }

}

sealed trait PathOrSingle extends PathOrValue

sealed trait PathOrValue

sealed trait Value extends PathOrValue

sealed trait Singular extends Value with PathOrSingle

case class InterpolatedString(value:String) extends Singular {
  val index = Map[String, String] = {

  }
}
case class SingleString(value: String) extends Singular

case class SingleFloat(value: Double) extends Singular

case class SingleBool(value: Boolean) extends Singular

case class ArrayValue(value: List[Value]) extends Value

case class ObjectValue(value: List[Member]) extends Value

case class NamedObjectValue(name: String, value: List[Member]) extends Value with Body {
  def members :  Map[String, Value] = value.map { x=>
    (x.key, x.value)
  }.toMap
}


case class Member(key: String, value: Value)

case class Path(value: List[String]) extends PathOrSingle

sealed trait ReVal

case class ReLiteral(re: String) extends ReVal

case class ReString(re: String) extends ReVal

sealed trait In extends Condition

case class InOp(left: PathOrSingle, right: PathOrValue) extends In

case class NotInOp(left: PathOrSingle, right: PathOrValue) extends In

object EqOp extends Compare

object NeOp extends Compare

object LtOp extends Compare

object LteOp extends Compare

object GtOp extends Compare

object GteOp extends Compare

sealed trait Compare

sealed trait Condition

case class Comparison(left: PathOrSingle, compare: Compare, right: PathOrSingle) extends Condition

case class ReCompare(left: PathOrSingle, right: ReVal) extends Condition

case class ReNotCompare(left: PathOrSingle, right: ReVal) extends Condition

sealed trait BooleanOp

object AndOp extends BooleanOp

object OrOp extends BooleanOp

object NandOp extends BooleanOp

object XorOp extends BooleanOp

case class PredicateCondition(booleanOp: BooleanOp, condition: Condition) extends Condition

case class CompoundCondition(condition: Condition, next: PredicateCondition) extends Condition

case class NegativeCondition(condition: Condition) extends Condition

sealed trait Body

sealed trait Conditional extends Body

case class Else(body: List[Body]) extends Conditional

case class IfCond(condition: Condition, body: List[Body], elseIf:List[Conditional]) extends Conditional

case class BooleanPath(path:Path) extends Condition

class EntireConfig(val input: List[Body], val filter: List[Body], val output: List[Body]) {

}