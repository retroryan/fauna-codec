package fauna

import faunadb.{FaunaClient, query}
import faunadb.query.{Arr, Class, CreateClass, CreateIndex, Exists, Expr, Obj}
import faunadb.values.Codec
import faunadb.values._

import scala.concurrent.{Await, ExecutionContext, Future}
import User._

import scala.concurrent.duration.Duration

case class User(userId: String, username: String)

object User {

  implicit val userCodec: Codec[User] = Codec.caseClass[User]


  val USER = "user"
  val USER_BY_USERNAME = "user_by_username"


  def initUserSchema(faunaClient: FaunaClient)(implicit context: ExecutionContext) = {

    val createUserClass: Expr = query.CreateClass(Obj("name" -> USER))

    val userClassExists: Expr = query.Exists(Class(USER))

    val createUserIndex = query.CreateIndex(
      Obj(
        "name" -> USER_BY_USERNAME,
        "source" -> Class(USER),
        "terms" -> Arr(Obj("field" -> Arr("data", "username")))
      )
    )
    val userIndexExists: Expr = query.Exists(query.Index(USER_BY_USERNAME))

    println("init user class")

    val eventualTuple = for {
      x <- faunaClient.query(
        query.If(
          query.Not(query.Exists(Class(USER))),
          query.CreateClass(Obj("name" -> USER)),
          query.Get(query.Class(USER))
        )
      )
      y <- faunaClient.query(
        query.If(
          query.Not(userIndexExists),
          createUserIndex,
          query.Get(query.Index(USER_BY_USERNAME))
        )
      )
    } yield (x, y)

    printTPL(eventualTuple)

    println("Initialized user class")
  }

  def printTPL(fv: Future[(Value, Value)]) = {
    val (x, y) = Await.result(fv, Duration.Inf)
    println(s"X ->  $x")
    println
    println(s"Y ->  $y")
  }

}