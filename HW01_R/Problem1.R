V <- c(7,2,1,0,3,-1,-3,4); V
A <- matrix(V, nrow = 4, ncol = 2, byrow = TRUE); A
AT <- t(A); AT
A %*% AT
dim(A %*% AT)
AT %*% A
dim(AT %*% A)
solve(AT %*% A)
V <- c(V, -2); V
B <- matrix(V, nrow = 3, ncol = 3, byrow = TRUE); B
Binv <- solve(B); Binv
B %*% Binv
Binv %*% B
zapsmall(B %*% Binv) == zapsmall(Binv %*% B)
Beigen <- eigen(B); Beigen
Beigenvectors = Beigen$vectors; Beigenvectors
B %*% Beigenvectors
Beigenvectors %*% B
solve(Beigenvectors) %*% B %*% Beigenvectors
dimnames(B) <- list(c("R1", "R2", "R3"), c("C1", "C2", "C3")); B
class(B)
dataframe = data.frame(B)
class(dataframe)
is.data.frame(dataframe)
labels(dataframe)
names(dataframe)
row.names(dataframe)
