import matplotlib.pyplot as plt
import numpy as np

# Set a seed for reproducibility
np.random.seed(42)

# Generate random data points
xpoints = np.array([x for x in range(10)])
ypoints = np.array([np.random.randint(10) for x in range(10)])

# Plot the data points
plt.plot(xpoints, ypoints, label="Data Points")

# Add a horizontal dashed line at y=2
plt.axhline(y=2, color='r', linestyle='dashed', label="Red Line at y=2")

# Add labels and legend
plt.xlabel('X-axis')
plt.ylabel('Y-axis')
plt.title('Random Data Points with Red Line at y=2')
plt.legend()

# Show the plot
plt.show()

