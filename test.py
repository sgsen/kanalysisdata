import pandas as pd
import numpy as np
import matplotlib.pyplot as plot
import seaborn as sns;

sns.set(color_codes=True)

x = [2, 3, 4, 5, 6, 7]
z = range(10, 16)

print(x)
print(z)

y = pd.Series(x)
z = pd.Series(z)

print(y)
print(z)

m = np.mean(y)

print(m)

# plot.scatter(y, z**2)
# plot.show()
mean, cov = [4, 6], [(1.5, .7), (.7, 1)]
x, y = np.random.multivariate_normal(mean, cov, 80).T
ax = sns.regplot(x=x, y=y, color="g", marker="+")
plot.show()


def testfunction():
    print("Running Test Function")


testfunction()
