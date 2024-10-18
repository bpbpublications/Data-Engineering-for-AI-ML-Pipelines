// Implementing data imputation using a more sophisticated method

val imputedData = salesData.na.replace("sales", Map(850 -> 130, 800 -> 120))

imputedData.show()

// Validation by checking data range
val validatedData = salesData.filter($"sales" >= 50 && $"sales" <= 150)

validatedData.show()
