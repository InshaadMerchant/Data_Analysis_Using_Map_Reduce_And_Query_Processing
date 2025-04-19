import matplotlib.pyplot as plt
import numpy as np

# Data from the MapReduce output
data = {
    '1991-2000': {
        'Action,Thriller': 55,
        'Adventure,Drama': 56,
        'Comedy,Romance': 215
    },
    '2001-2010': {
        'Action,Thriller': 76,
        'Adventure,Drama': 141,
        'Comedy,Romance': 400
    },
    '2011-2020': {
        'Action,Thriller': 208,
        'Adventure,Drama': 343,
        'Comedy,Romance': 590
    }
}

# Set up the figure and axis
plt.figure(figsize=(12, 8))
ax = plt.subplot(111)

# Set up the bar positions
bar_width = 0.25
r1 = np.arange(len(data['1991-2000']))
r2 = [x + bar_width for x in r1]
r3 = [x + bar_width for x in r2]

# Plot the bars
plt.bar(r1, list(data['1991-2000'].values()), width=bar_width, label='1991-2000', color='#1f77b4')
plt.bar(r2, list(data['2001-2010'].values()), width=bar_width, label='2001-2010', color='#ff7f0e')
plt.bar(r3, list(data['2011-2020'].values()), width=bar_width, label='2011-2020', color='#2ca02c')

# Add labels and title
plt.xlabel('Genre Combinations', fontsize=12)
plt.ylabel('Number of Movies', fontsize=12)
plt.title('Movie Counts by Genre Combination and Time Period', fontsize=14, pad=20)

# Add x-axis labels
plt.xticks([r + bar_width for r in range(len(data['1991-2000']))], 
           ['Action,Thriller', 'Adventure,Drama', 'Comedy,Romance'],
           rotation=45, ha='right')

# Add legend
plt.legend()

# Add value labels on top of each bar
def add_value_labels(ax, spacing=5):
    for rect in ax.patches:
        y_value = rect.get_height()
        x_value = rect.get_x() + rect.get_width() / 2
        space = spacing
        va = 'bottom'
        label = "{:.0f}".format(y_value)
        ax.annotate(label, (x_value, y_value), xytext=(0, space), 
                    textcoords="offset points", ha='center', va=va)

add_value_labels(ax)

# Adjust layout to prevent label cutoff
plt.tight_layout()

# Save the plot
plt.savefig('movie_counts_bar_chart.png', dpi=300, bbox_inches='tight')
plt.close()