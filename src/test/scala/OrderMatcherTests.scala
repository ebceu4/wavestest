import org.scalatest._

import scala.collection.immutable.{List, SortedSet}

class OrderMatcherTests extends FlatSpec with Matchers {

  def processOrders(orders: Vector[Order]): (InstrumentBook, List[BalanceChange]) ={
    orders.foldLeft((InstrumentBook.empty, List[BalanceChange]()))(
      (state, order) =>
      {
        val (book, balanceChanges) = state
        val (newBook, newBalanceChanges) = OrderMatcher.placeOrder(book, order)

        (newBook, balanceChanges ++ newBalanceChanges)
      }
    )
  }

  "An orders with the same price" should "be ordered chronologically" in {

    val set = SortedSet[Order](
      Order("C2", "", OrderType.Sell, 1, 1),
      Order("C1", "", OrderType.Sell, 1, 1))

    set.head shouldEqual Order("C2", "", OrderType.Sell, 1, 1)
  }

  "A sell orders with the different prices" should "be ordered by price ascending" in {

    val set = SortedSet[Order](
      Order("C2", "", OrderType.Sell, 2, 1),
      Order("C1", "", OrderType.Sell, 1, 1))

    set.head shouldEqual Order("C1", "", OrderType.Sell, 1, 1)
  }

  "A buy orders with the different prices" should "be ordered by price descending" in {

    val set = SortedSet[Order](
      Order("C2", "", OrderType.Buy, 1, 1),
      Order("C1", "", OrderType.Buy, 2, 1))

    set.head shouldEqual Order("C1", "", OrderType.Buy, 2, 1)
  }

  "An orders with the different quantity" should "not be matched" in {

    val orders = Vector(
      Order("C1", "", OrderType.Sell, 1, 2),
      Order("C2", "", OrderType.Buy, 1, 1)
    )

    val (book, balanceChanges) = processOrders(orders)

    book.sell.head shouldEqual orders(0)
    book.buy.head shouldEqual orders(1)
    balanceChanges.isEmpty shouldBe true
  }

  "An orders from the same client" should "not be matched" in {

    val orders = Vector(
      Order("C1", "", OrderType.Sell, 1, 1),
      Order("C1", "", OrderType.Buy, 1, 1)
    )

    val (book, balanceChanges) = processOrders(orders)

    book.sell.head shouldEqual orders(0)
    book.buy.head shouldEqual orders(1)
    balanceChanges.isEmpty shouldBe true
  }

  "An orders with the price gap" should "not be matched" in {

    val orders = Vector(
      Order("C1", "", OrderType.Sell, 20, 1),
      Order("C2", "", OrderType.Buy, 10, 1)
    )

    val (book, balanceChanges) = processOrders(orders)

    book.sell.head shouldEqual orders(0)
    book.buy.head shouldEqual orders(1)
    balanceChanges.isEmpty shouldBe true
  }

  "An orders with the same price and quantity from different clients" should "be matched" in {

    val orders = Vector(
      Order("C1", "", OrderType.Sell, 1, 1),
      Order("C2", "", OrderType.Buy, 1, 1)
    )

    val (book, balanceChanges) = processOrders(orders)

    book.buy.isEmpty shouldBe true
    book.sell.isEmpty shouldBe true
    balanceChanges.length shouldBe 2
  }

  "An orders with the price overlap" should "be matched" in {

    val orders = Vector(
      Order("C1", "", OrderType.Sell, 10, 1),
      Order("C2", "", OrderType.Buy, 20, 1)
    )

    val (book, balanceChanges) = processOrders(orders)

    book.buy.isEmpty shouldBe true
    book.sell.isEmpty shouldBe true
    balanceChanges.length shouldBe 2
  }

  "First balance change entry" should "correspond to the same client whose order is placed" in {

    val orders = Vector(
      Order("C1", "", OrderType.Sell, 1, 1),
      Order("C2", "", OrderType.Buy, 1, 1)
    )

    val (_, balanceChanges) = processOrders(orders)

    balanceChanges.head.client shouldBe "C2"
  }

  "Second balance change entry" should "correspond to the client whose order was in book already" in {

    val orders = Vector(
      Order("C1", "", OrderType.Sell, 1, 1),
      Order("C2", "", OrderType.Buy, 1, 1)
    )

    val (_, balanceChanges) = processOrders(orders)

    balanceChanges(1).client shouldBe "C1"
  }

  "A pair of matched orders" should "produce corresponding instrument balance change for both clients" in {

    val orders = Vector(
      Order("C1", "", OrderType.Sell, 1, 1),
      Order("C2", "", OrderType.Buy, 1, 1)
    )


    val (_, balanceChanges) = processOrders(orders)

    balanceChanges.head shouldEqual BalanceChange("C2", +1, -1)
    balanceChanges(1) shouldEqual BalanceChange("C1", -1, +1)
  }

  "Money balance changes" should "be calculated by the order price that was already presented in book" in {

    val orders = Vector(
      Order("C1", "", OrderType.Buy, 20, 1),
      Order("C2", "", OrderType.Sell, 10, 1)
    )

    val (_, balanceChanges) = processOrders(orders)

    balanceChanges.head shouldEqual BalanceChange("C2", -1, +20)
    balanceChanges(1) shouldEqual BalanceChange("C1", +1, -20)
  }

  "An order" should "be matched for the best available price" in {

    val orders = Vector(
      Order("C1", "", OrderType.Sell, 1, 1),
      Order("C1", "", OrderType.Sell, 2, 1),
      Order("C2", "", OrderType.Buy, 20, 1)
    )

    val (_, balanceChanges) = processOrders(orders)

    balanceChanges.head shouldEqual BalanceChange("C2", +1, -1)
  }

  "An order" should "be matched for the fist opposite side order if there are several suitable ones" in {

    val orders = Vector(
      Order("C3", "", OrderType.Sell, 1, 1),
      Order("C1", "", OrderType.Sell, 1, 1),
      Order("C2", "", OrderType.Buy, 20, 1)
    )

    val (_, balanceChanges) = processOrders(orders)

    balanceChanges.head shouldEqual BalanceChange("C2", +1, -1)
    balanceChanges(1) shouldEqual BalanceChange("C3", -1, +1)
  }
}