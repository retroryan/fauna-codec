package fauna

import faunadb.{FaunaClient, query}
import faunadb.query.{Expr, _}

import scala.concurrent.{ExecutionContext, Future}

class RepositoryAdmin(client: FaunaClient)(implicit context: ExecutionContext) {

  /** Index by Contract ID **/
  val createIndexById: Expr = {
    query.CreateIndex(
      query.Obj(
        "name" -> "contract_by_id",
        "source" -> Class("contract"),
        "serialized" -> true,
        "unique" -> false,
        "terms" -> Arr(Obj("field" -> Arr("data", "contract_id"))),
        "values" -> Arr(
          Obj("field" -> Arr("data", "contract_id")),
          Obj("field" -> Arr("ref"))
        )
      )
    )
  }
  val indexByIdExists: Expr = query.Exists(query.Index("contract_by_id"))

  /** Contract Class **/
  val createContractClass: Expr = query.CreateClass(query.Obj("name" -> "contract"))
  val contractClassExists: Expr = query.Exists(query.Class("contract_by_id"))

  def faunaInit(): Future[Unit] = {
    for {
      x <- client.query(
        query.If(
          query.Not(contractClassExists),
          createContractClass,
          query.Get(query.Class("contract"))
        )
      )
      y <- client.query(
        query.If(
          query.Not(indexByIdExists),
          createIndexById,
          query.Get(query.Class("contract"))
        )
      )
    } yield ()
  }

}
