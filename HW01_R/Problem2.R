mydata <- read.csv("~/Downloads/2006Data.csv", header = TRUE, sep = ",", colClasses = c(NA, NA, NA, NA, NA, NA, NA, "NULL"))
summary(mydata)
class(mydata)
power = mydata$Power
temperature = mydata$Temperature
hour = mydata$Hour
plot(temperature, power, xlab = "Temperature", ylab = "Power consumption")
plot(hour, power, xlab = "Hour of the day", ylab = "Power consumption")
boxplot(Power ~ Hour, data = mydata, main = "The distribution of power consumption for every hour of the day", xlab = "Hour of the day", ylab = "Power consumption")
