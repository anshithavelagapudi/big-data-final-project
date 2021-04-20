# big-data-final-project

Process text using Databricks community Edition and pySpark.

# Data Bricks Link

# Input Source
[Data url](https://www.gutenberg.org/ebooks/74)
[Text url](https://www.gutenberg.org/files/74/74-0.txt)

# Tools/Languages used:
- Languages : Python
- Tools: Databricks Notebook, Pyspark, Regex, Pandas, MatPlotLib, Seaborn, Urllib

# Steps to follow
## Step-1: Data Injection
- Import all libraries and fetch the data from url

<pre lang="no-highlight"> 
<code> import urllib.request 
stringInURL = "https://www.gutenberg.org/files/74/74-0.txt"
urllib.request.urlretrieve(stringInURL,"/tmp/adventures.txt")
</code>

- Relocate the file from temp folder to databricks folder of dbfs
<pre lang="no-highlight"> 
<code>
dbutils.fs.mv("file:/tmp/adventures.txt","dbfs:/data/adventures.txt")
</code>

- Transfer the data into Spark using sparkContext
<pre lang="no-highlight"> 
<code>
adventuresRDD= sc.textFile("dbfs:/data/adventures.txt")
</code>

## Step-2: Clean the data
- Now, we separate the words from each line using flatmap function and to change all the words to lower case and remove the spaces between them
```python
adventuresMessyTokensRDD = adventuresRDD.flatMap(lambda eachLine: eachLine.lower().strip().split(" "))
```
- After changing the words to lowercase, we need to remove punctuations using regex by importing regex library
```python
import re
wordsAfterCleanedTokensRDD = adventuresMessyTokensRDD.map(lambda letter: re.sub(r'[^A-Za-z]', '', letter))
```
- After that, by filtering the data, remove all the stop words from the data and create a new RDD with the new result
```python
from pyspark.ml.feature import StopWordsRemover
remover = StopWordsRemover()
stopwords = remover.getStopWords()
adventuresWordsRDD = wordsAfterCleanedTokensRDD.filter(lambda word: word not in stopwords)
# removing all the empty spaces from the data
adventuresRemoveSpaceRDD = lifeAndWorkadventuresWordsRDD.filter(lambda x: x != "")
```
## Step-3:- Process the data
- In this step, we will pair up each word in file and count it as 1 as an intermediate Key-value pairs and we need to transform the words using reduceByKey() to get the total count of all distinct words. To get back to python, we use collect() and then print the obtained results.
```python
adventuresPairsRDD = adventuresRemoveSpaceRDD.map(lambda eachWord: (eachWord,1))
# transforming the words using reduceByKey() to get (word,count) results
adventuresWordCountRDD = adventuresPairsRDD.reduceByKey(lambda acc, value: acc + value)
#collect() action to get back to python
results = adventuresWordCountRDD.collect()
print(results)
```
- Sort the words based on high word count and display them in descending order
```python
output = sorted(results, key=lambda t: t[1], reverse=True)[:15]
print(output)
```
## Charting the data
* Display the data by ploting the obtained output, where we need to import the required libraries and then label the axis as per the requirement.  
```python
import pandas as pd  
import matplotlib.pyplot as plt
import seaborn as sns

# preparing chart information
source = 'The Project Gutenberg eBook of The Adventures of Tom Sawyer, by Mark Twain'
title = 'Top Words in ' + source
xlabel = 'Words'
ylabel = 'Count'

df = pd.DataFrame.from_records(output, columns =[xlabel, ylabel]) 
plt.figure(figsize=(20,4))
sns.barplot(xlabel, ylabel, data=df, palette="cubehelix").set_title(title)
```

