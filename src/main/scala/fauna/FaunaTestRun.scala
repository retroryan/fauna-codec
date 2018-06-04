package fauna


import faunadb.{FaunaClient, query}
import faunadb.query._
import faunadb.values._

import scala.concurrent.{Future, _}
import scala.concurrent.duration._
import User._
import Contract._


object FaunaTestRun {

  def main(args: Array[String]): Unit = {
    val FAUNA_URL = "http://127.0.0.1:8443"
    val faunaClient = FaunaClient("secret", FAUNA_URL)

    import scala.concurrent.ExecutionContext.Implicits.global
    FaunaTestRun.testAddUsers(faunaClient)
    FaunaTestRun.testAddContracts(faunaClient)
    FaunaTestRun.testQueryContracts(faunaClient)
    FaunaTestRun.testRedeemContracts(faunaClient)

    faunaClient.close()
  }

  def printF(name: String, fv: Future[Any]) = {
    println(
      s"$name ->  ${Await.result(fv, Duration.Inf)}"
    )
  }

  def testAddUsers(client: FaunaClient)(implicit context: ExecutionContext) = {

    User.initUserSchema(client)
    Contract.initContractSchema(client)

    val testUser = client.query(
      Create(
        Class(USER),
        Obj(
          "data" -> Obj(
            "userId" -> "453",
            "username" -> "Barnaby"),
          )
        )
    )

    println("save a person")
    val u1 = User("100", "hydro")
    val u2 = User("110", "fall")
    val u3 = User("120", "admin")
    val u4 = User("130", "alice")
    val u5 = User("140", "bob")


    val newUsers = client.query(
      Foreach(
        Arr(u1, u2, u3, u4, u5),
        Lambda { user =>
          Create(
            Class(USER),
            Obj("data" -> user))
        }))

    printF("new users", newUsers)


    println("Testing query user")
    val r1 = client.query(
      query.Let {
        val refs = Select("ref", Get(Match(Index(USER_BY_USERNAME), "hydro")))
        Select("data", Get(refs))
      }
    ).map(value => value.to[User].get)
    printF("query user", r1)
  }

  def testAddContracts(client: FaunaClient)(implicit context: ExecutionContext) = {

    val firstContract = createContract("345", "hydro", "alice", "4572.32", "PROPOSED", client)
    printF("first contract", firstContract)
    val secondContract = createContract("347", "hydro", "bob", "81572.32", "PROPOSED", client)
    printF("second contract", secondContract)

  }

  def testRedeemContracts(client: FaunaClient)(implicit context: ExecutionContext) = {

    try {
      println("TEST redeem contract")
      redeemContract("347", client)
    }
    catch {
      case exc => println(exc, exc.printStackTrace())
    }

  }

  def testQueryContracts(client: FaunaClient)(implicit context: ExecutionContext) = {

    println("Query by contract ID test")

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

    try {
      val data = Await.result(futureSingleContract, Duration.Inf)
      val creditor = data("creditor").map(value => value.to[User].get).get
      val debtor = data("debtor").map(value => value.to[User].get).get
      val contractVal = data("contract")
      val faunaContract = contractVal.map(value => value.to[FaunaContract].get).get
      val finalContract = Contract(faunaContract, creditor, debtor)
      println(s"finalContract: $finalContract")
    } catch {
      case exc => println("ERROR", exc)
    }

    println("Query by contract ID list test")

    try {
      val futureListContracts = client.query(
        Let {
          val contractsByCreditors =
            Paginate(
              Match(
                Index(CONTRACT_BY_CREDITOR),
                Select(
                  "ref",
                  Get(Match(Index(USER_BY_USERNAME), "hydro"))))
            )
          Map(
            contractsByCreditors,
            Lambda { contractRef =>
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
        }
      )
      //printF("joined contract by creditors", futureListContracts)

      println
      println

      // val data = Await.result(futureListContracts, Duration.Inf)
      val lstContracts = futureListContracts.map { listContracts =>

        val arrayV = listContracts("data").map(value => value.to[ArrayV].get).get

        val allContracts = arrayV.elems.map { nextElem =>

          val creditor = nextElem("creditor").map(value => value.to[User].get).get
          val debtor = nextElem("debtor").map(value => value.to[User].get).get
          val contractVal = nextElem("contract")
          val faunaContract = contractVal.map(value => value.to[FaunaContract].get).get
          val finalContract = Contract(faunaContract, creditor, debtor)
          finalContract
        }
        allContracts
      }

      lstContracts.foreach(_.foreach(println))
      Await.result(lstContracts, Duration.Inf)

    } catch {
      case exc => println("ERROR", exc)
    }


  }
}
