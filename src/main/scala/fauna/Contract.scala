package fauna

import faunadb.{FaunaClient, query}
import faunadb.query.{Arr, Class, Create, CreateClass, CreateIndex, Exists, Expr, Get, Index, Match, Obj, Select}
import faunadb.values.Codec
import faunadb.values._
import faunadb.query._

import scala.concurrent._
import scala.concurrent.duration.Duration

import User._

/**
  * How do we ensure we only move between valid states,?
  *
  * state is a DAG and can only go one direction from PROPOSED -> ACCEPTED -> REDEEMED -> REJECTED
  * PROPOSED -> 1
  * ACCEPTED -> 2
  * REDEEMED -> 3
  * REJECTED -> 4
  *
  */
case class FaunaContract(contractId: String, creditor: RefV, debtor: RefV, amount: String, state: String)

case class Contract(contractId: String, creditor: User, debtor: User, amount: String, state: String)

object Contract {

  implicit val contractCodec: Codec[FaunaContract] = Codec.caseClass[FaunaContract]

  def apply(faunaContract: FaunaContract, creditor: User, debtor: User): Contract = Contract(faunaContract.contractId, creditor, debtor, faunaContract.amount, faunaContract.state)


  val CONTRACT = "contract"
  val CONTRACT_BY_ID = "contract_by_id"
  val CONTRACT_BY_CREDITOR = "contract_by_creditor"
  val CONTRACT_BY_DEBTOR = "contract_by_debtor"




  def createContract(contractId: String, creditor: String,
                     debtor: String, amount: String,
                     state: String, client: FaunaClient)(implicit context: ExecutionContext): Future[Value] = {
    client.query(
      Create(
        Class(CONTRACT),
        Obj(
          "data" -> Obj(
            "contractId" -> contractId,
            "creditor" -> Select("ref", Get(Match(Index(USER_BY_USERNAME), creditor))),
            "debtor" -> Select("ref", Get(Match(Index(USER_BY_USERNAME), debtor))),
            "amount" -> amount,
            "state" -> state
          )
        )))
  }

  def redeemContract(contractId: String, client: FaunaClient)(implicit context: ExecutionContext) = {

    printF("Contract" , queryContract(contractId, client))


    //This does not check the contract state before redeemeding the contract.
    //Needs to be updated to do an if check
    val update = client.query(
      query.Let {
        val contract = Get(Match(Index(CONTRACT_BY_ID), contractId))
        Update(Select(Arr("ref"), contract),
          Obj("data" -> Obj("state" -> "REDEEMED")))
      }
    )

    printF("update results", update)

  /*  val redeemResults2 = client.query(
      query.Let {
        val contract = Get(Match(Index(CONTRACT_BY_ID), contractId))
        query.Let {
          val state = Select(Arr("data", "state"), contract)
          If(Equals(state, "REDEEMED"),
            Arr("Error Already REDEEMED"),
            Do(
              Update(Select(Arr("ref"), contract),
                Obj("data" -> Obj("state" -> "REDEEMED")))
            )
          )
        }
      }
    )

    printF("redeem results", redeemResults2)
    redeemResults2*/
  }

  def initContractSchema(client: FaunaClient)(implicit context: ExecutionContext) = {

    val createContract: Expr = CreateClass(Obj("name" -> CONTRACT))

    val contractExists: Expr = Exists(Class(CONTRACT))

    val contractIndex = CreateIndex(
      Obj(
        "name" -> CONTRACT_BY_ID,
        "source" -> Class(CONTRACT),
        "terms" -> Arr(Obj("field" -> Arr("data", "contractId")))
      )
    )
    val contractIndexExists: Expr = query.Exists(query.Index(CONTRACT_BY_ID))

    val contractByCreditorIndex = CreateIndex(
      Obj(
        "name" -> CONTRACT_BY_CREDITOR,
        "source" -> Class(CONTRACT),
        "terms" -> Arr(Obj("field" -> Arr("data", "creditor")))
      )
    )
    val contractByCreditorIndexExists: Expr = query.Exists(query.Index(CONTRACT_BY_CREDITOR))


    val contractByDeptorIndex = CreateIndex(
      Obj(
        "name" -> CONTRACT_BY_DEBTOR,
        "source" -> Class(CONTRACT),
        "terms" -> Arr(Obj("field" -> Arr("data", "debtor"))),
        "values" -> Arr(Obj("field" -> Arr("data", "contractId")))
      )
    )
    val contractByDeptorIndexExists: Expr = query.Exists(query.Index(CONTRACT_BY_DEBTOR))



    val future1 = for {
      t <- client.query(
        query.If(
          query.Not(contractExists),
          createContract,
          query.Get(query.Class(CONTRACT))
        )
      )
      u <- client.query(
        query.If(
          query.Not(contractIndexExists),
          contractIndex,
          query.Get(query.Index(CONTRACT_BY_ID))
        )
      )
      v <- client.query(
        query.If(
          query.Not(contractByCreditorIndexExists),
          contractByCreditorIndex,
          query.Get(query.Index(CONTRACT_BY_CREDITOR))
        )
      )
      w <- client.query(
        query.If(
          query.Not(contractByDeptorIndexExists),
          contractByDeptorIndex,
          query.Get(query.Index(CONTRACT_BY_DEBTOR))
        )
      )
    } yield (t, u, v, w)

    println("Initialized contract class")
  }

  def queryContract(contractID:String, client: FaunaClient)(implicit context: ExecutionContext): Future[Contract] = {

    val futureSingleContract = client.query(
      query.Let {
        val contractRef = Select("ref", Get(Match(Index(CONTRACT_BY_ID), "345")))
        Let {
          val contract = Get(contractRef)
          Let {
            val creditorRef = Select(Arr("data", "creditor"), contract)
            val debtorRef = Select(Arr("data", "debtor"), contract)
            Obj("creditor" -> Select(Arr("data"), Get(creditorRef)),
              "debtor" -> Select(Arr("data"), Get(debtorRef)),
              "contract" -> Select(Arr("data"), Get(contractRef)))
          }
        }
      }
    )

    futureSingleContract.map {data =>
      val creditor = data("creditor").map(value => value.to[User].get).get
      val debtor = data("debtor").map(value => value.to[User].get).get
      val contractVal = data("contract")
      val faunaContract = contractVal.map(value => value.to[FaunaContract].get).get
      Contract(faunaContract, creditor, debtor)
    }
  }
  def printF(name: String, fv: Future[Any]) = {
    println(
      s"$name ->  ${Await.result(fv, Duration.Inf)}"
    )
  }

}