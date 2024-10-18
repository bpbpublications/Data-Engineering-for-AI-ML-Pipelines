import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel}

val spark = SparkSession.builder()
.appName("Deequ Data Validation")
.config("spark.master", "local")
.getOrCreate()

import spark.implicits._
// Sample DataFrame
val customerData = Seq(
	(1, "John Doe", "john.doe@example.com"),
	(2, "Jane Smith", "jane.smith@example.com"),
	(3, null, "jake.jones@example.com"),
	(4, "Jessie James", "jessie.james@example.com")).toDF("customer_id", "name", "email")

// Using Deequ to define and run data quality checks
val verificationResult = VerificationSuite()
	.onData(customerData)
	.addCheck(
		Check(CheckLevel.Error, "Data Quality Checks")
			.hasSize(_ >= 4) // Expect at least 4 records.isComplete("name") // No nulls in the 'name' column
			.isUnique("customer_id") // Customer ID should be unique
			.isComplete("email") // No nulls in the 'email' column
			.satisfies("customer_id > 0", "Positive IDs") // All IDs should be positive
		)
	.run()

// Displaying the results of the data quality checks
if (verificationResult.status == com.amazon.deequ.VerificationResult.Success) {
	println("Data quality checks passed.")
} else {
	verificationResult.checkResults
		.foreach { case (_, checkResult) =>
			println(s"Check ${checkResult.constraint} failed:${checkResult.message.getOrElse("")}")
		}
	}



