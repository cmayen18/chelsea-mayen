# The following code to create a dataframe and remove duplicated rows is always executed and acts as a preamble for your script: 

# dataset = pandas.DataFrame(PCA1, PCA2, cluster)
# dataset = dataset.drop_duplicates()

# Paste or type your script code here:

import seaborn as sns
import matplotlib.pyplot as plt

# Scatter plot with clusters
sns.scatterplot(x='PCA1', y='PCA2', hue='cluster', data=dataset, palette='viridis', legend='full')
plt.title('K-Means Clustering of Spotify Tracks')
plt.xlabel('PCA1')
plt.ylabel('PCA2')
plt.show()
