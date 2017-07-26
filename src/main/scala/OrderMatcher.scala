import java.io.PrintWriter

import scala.collection.immutable.{List, Map, SortedSet}
import scala.io.Source

case class Order(client: String, asset: String, orderType: OrderType.Type, price: Int, quantity: Int) extends Ordered [Order] {
  def compare (that: Order) : Int = {
    if (this.price == that.price && this.client == that.client && this.quantity == that.quantity && this.orderType == that.orderType)
      return 0

    if(this.price == that.price)
      return 1

    (if (this.price > that.price)
      1
    else
      -1) * (if (this.orderType == OrderType.Buy) -1 else 1)
  }
}

case class Client(id: String, money: Int, assets: Map[String, Int])
case class BalanceChange(client: String, assetChange: Int, moneyChange: Int)
case class Exchange(clients: Map[String, Client], books: Map[String, InstrumentBook])
case class InstrumentBook(buy: SortedSet[Order], sell: SortedSet[Order])

object InstrumentBook {
  val empty = InstrumentBook(SortedSet[Order](), SortedSet[Order]())
}

object OrderType {
  sealed trait Type
  case object Buy extends Type
  case object Sell extends Type
}

object OrderMatcher extends App
{
  val assets = Seq("A", "B", "C", "D")

  def placeOrder(book: InstrumentBook, orderToPlace: Order): (InstrumentBook, List[BalanceChange]) = {

    if (orderToPlace.orderType == OrderType.Buy) {
      if (book.sell.isEmpty || book.sell.last.price > orderToPlace.price) {
        (book.copy(buy = book.buy + orderToPlace), List.empty)
      }
      else {
        book.sell.find(o => o.quantity == orderToPlace.quantity && o.client != orderToPlace.client) match {
          case Some(order) => (book.copy(sell = book.sell - order),
            List(
              BalanceChange(orderToPlace.client, orderToPlace.quantity, -orderToPlace.quantity * order.price),
              BalanceChange(order.client, -orderToPlace.quantity, orderToPlace.quantity * order.price)
            ))
          case None => (book.copy(buy = book.buy + orderToPlace), List.empty)
        }
      }
    }
    else {
      if (book.buy.isEmpty || book.buy.last.price < orderToPlace.price) {
        (book.copy(sell = book.sell + orderToPlace), List.empty)
      }
      else {
        book.buy.find(o => o.quantity == orderToPlace.quantity && o.client != orderToPlace.client) match {
          case Some(order) => (book.copy(buy = book.buy - order),
            List(
              BalanceChange(orderToPlace.client, -orderToPlace.quantity, orderToPlace.quantity * order.price),
              BalanceChange(order.client, orderToPlace.quantity, -orderToPlace.quantity * order.price)
            ))
          case None => (book.copy(sell = book.sell + orderToPlace), List.empty)
        }
      }
    }
  }

  def readClients(path: String): Map[String, Client] =
    readLines(path).map(line => parseClient(line)).map(c => (c.id, c)).toMap

  def writeClients(path: String, clients: Seq[Client]) =
    new PrintWriter(path) {
      clients.sortBy(c => c.id).map(c => s"${c.id}\t${c.money}\t${c.assets.values.map(_.toString).mkString("\t")}").foreach(c => write(c.toString + "\n"))
      close()
    }

  def readOrders(path: String): Iterator[Order] =
    readLines(path).map(line => parseOrder(line))

  def readLines(path: String): Iterator[String] =
    Source.fromFile(path).getLines()

  def parseClient(line: String): Client = {
    val parts = line.split('\t')

    Client(parts.head,
      parts(1).toInt,
      parts.drop(2).map{ _.toInt }.zip(assets).map{case (value, asset) => (asset, value)}.toMap
    )
  }

  def parseOrder(line: String): Order = {
    val parts = line.split('\t')

    Order(
      parts(0),
      parts(2),
      if(parts(1) == "b") OrderType.Buy else OrderType.Sell,
      parts(3).toInt,
      parts(4).toInt)
  }

  val clients = readClients(args(0))
  val exchange = Exchange(clients, assets.map(a => (a, InstrumentBook.empty)).toMap)

  val finalExchangeState = readOrders(args(1)).foldLeft(exchange) ((exchange, order) => {

    val (newBook, balanceChanges) = placeOrder(exchange.books(order.asset), order)

    val newClients = (for(bc <- balanceChanges; client = exchange.clients(bc.client)) yield
      bc.client -> client.copy(money = client.money + bc.moneyChange, assets = client.assets + (order.asset -> (client.assets(order.asset) + bc.assetChange))))
      .foldLeft(exchange.clients)((clients, tuple) => clients + tuple)

    exchange.copy(clients = newClients, books = exchange.books + (order.asset -> newBook))
  })

  writeClients(args(2), finalExchangeState.clients.values.toSeq)
}


