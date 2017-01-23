breaks = seq(1.0, 94.0, by = 1.86); breaks;
temperature.cut = cut(temperature, breaks, right=FALSE);
maxpower = tapply(power, temperature.cut, max); maxpower
minpower = tapply(power, temperature.cut, min); minpower
meanpower = tapply(power, temperature.cut, mean); meanpower
midtemperature = tapply(temperature, temperature.cut, median); midtemperature
plot(midtemperature, meanpower, main = "Distribution of power consumption for different temperature", xlab = "Temperature", ylab = "Power consumption", xlim = c(0, 100), ylim = c(40, 110), col = "green")
points(midtemperature, maxpower, col = "red")
points(midtemperature, minpower, col = "blue")
cov(midtemperature, maxpower)
cov(midtemperature, minpower)
cov(midtemperature, meanpower)
M <- cbind(midtemperature, maxpower, minpower, meanpower)
cov(M)
