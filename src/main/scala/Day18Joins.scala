
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day18Joins {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 18 Orders Customers Payments Joins")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    // Customers
    val customers = Seq(
      (1, "Ravi", "Hyderabad"),
      (2, "Priya", "Chennai"),
      (3, "Arjun", "Bangalore"),
      (4, "Sneha", "Mumbai")
    ).toDF("customer_id", "customer_name", "city")

    // Orders
    val orders = Seq(
      (101, 1, 2500.0),
      (102, 2, 4000.0),
      (103, 1, 1500.0),
      (104, 5, 3000.0),
      (105, 3, 5000.0)
    ).toDF("order_id", "customer_id", "order_amount")

    // Payments
    val payments = Seq(
      (101, "Paid", 2500.0),
      (102, "Paid", 4000.0),
      (104, "Pending", 1000.0),
      (106, "Paid", 1800.0)
    ).toDF("order_id", "payment_status", "payment_amount")

    println("\n========== CUSTOMERS ==========")
    customers.show()

    println("\n========== ORDERS ==========")
    orders.show()

    println("\n========== PAYMENTS ==========")
    payments.show()

    // Aliases prevent ambiguity when column names overlap
    val o = orders.alias("o")
    val c = customers.alias("c")
    val p = payments.alias("p")

    // 1. INNER JOIN: matching orders and customers
    println("\n========== INNER JOIN ==========")

    o.join(
      c,
      col("o.customer_id") === col("c.customer_id"),
      "inner"
    ).select(
      col("o.order_id"),
      col("c.customer_name"),
      col("c.city"),
      col("o.order_amount")
    ).orderBy("order_id").show()

    // 2. LEFT JOIN: retain every order
    println("\n========== LEFT JOIN + NULL HANDLING ==========")

    val orderCustomer = o.join(
      c,
      col("o.customer_id") === col("c.customer_id"),
      "left"
    )

    orderCustomer.select(
      col("o.order_id"),
      col("o.customer_id"),
      coalesce(col("c.customer_name"), lit("Unknown"))
        .as("customer_name"),
      col("o.order_amount")
    ).orderBy("order_id").show()

    // 3. RIGHT JOIN: retain every customer
    println("\n========== RIGHT JOIN ==========")

    o.join(
      c,
      col("o.customer_id") === col("c.customer_id"),
      "right"
    ).select(
      col("o.order_id"),
      col("c.customer_id"),
      col("c.customer_name")
    ).orderBy("customer_id").show()

    // 4. FULL OUTER JOIN: retain all orders and customers
    println("\n========== FULL OUTER JOIN ==========")

    o.join(
      c,
      col("o.customer_id") === col("c.customer_id"),
      "full_outer"
    ).select(
      col("o.order_id"),
      coalesce(col("o.customer_id"), col("c.customer_id"))
        .as("customer_id"),
      col("c.customer_name"),
      col("o.order_amount")
    ).orderBy("order_id").show()

    // 5. Join orders with payments
    println("\n========== ORDERS WITH PAYMENT STATUS ==========")

    val orderPayments = o.join(
      p,
      col("o.order_id") === col("p.order_id"),
      "left"
    )

    orderPayments.select(
      col("o.order_id"),
      col("o.customer_id"),
      col("o.order_amount"),
      coalesce(col("p.payment_status"), lit("No Payment"))
        .as("payment_status"),
      col("p.payment_amount")
    ).orderBy("order_id").show()

    // 6. Complete report: orders + customers + payments
    println("\n========== COMPLETE ORDER REPORT ==========")

    o.join(
      c,
      col("o.customer_id") === col("c.customer_id"),
      "left"
    ).join(
      p,
      col("o.order_id") === col("p.order_id"),
      "left"
    ).select(
      col("o.order_id"),
      coalesce(col("c.customer_name"), lit("Unknown"))
        .as("customer_name"),
      col("c.city"),
      col("o.order_amount"),
      coalesce(col("p.payment_status"), lit("No Payment"))
        .as("payment_status"),
      col("p.payment_amount")
    ).orderBy("order_id").show()

    // 7. Find orders without a matching payment
    println("\n========== ORDERS WITHOUT PAYMENTS ==========")

    orderPayments
      .filter(col("p.order_id").isNull)
      .select(
        col("o.order_id"),
        col("o.order_amount")
      )
      .show()

    // 8. Explain Spark's physical join plan
    println("\n========== JOIN EXECUTION PLAN ==========")

    o.join(
      c,
      col("o.customer_id") === col("c.customer_id"),
      "inner"
    ).explain("formatted")

    spark.stop()
  }
}
