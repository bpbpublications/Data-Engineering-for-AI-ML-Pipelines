// Filling missing values with a specified value
val filledDf = df.na.fill(30, Seq("age"))

filledDf.show()

// Calculating the mean age to fill missing values
val meanAge = df.select("age").na.drop().agg(avg("age")).first().getDouble(0)
val meanFilledDf = df.na.fill(meanAge, Seq("age"))

meanFilledDf.show()
