# BigData
Backup the exercise  
###WordCount.java
####Do we need to create a new method for the combiner? Explain why/why not.
No, because the operation "Sum" is both commutative and associative. In this case,the combiner is a mini reducer that performs the local reduce task. They just do the same job under different scale, so we do not need to create a new method for the combiner.

####How would you modify this program so that it counts the word-length frequencies instead?
When counts the word frequencies, we return the "word" as key and the "one" as value. If we want to counts the word-length frequencies, just return the "word-length" as key instead of the "word". Just replace the line:
```java
word.set(itr.nextToken());
```
with another line:
```java
word.set(Integer.toString(itr.nextToken().length()));
```
if we do not care the naming convention.

###MaxWordCount.java
####How do we ensure that all entries in the 2nd stage go to the same reducer?
Use the dummy key. We can create a key and let all word-freq pairs to be the value. In this way, all entries, the word-freq pairs, have the same key, so they can go to the same reducer.
####Do we need to create a separate combiner for the 2nd stage? Explain why/why not.
No. Like the "Sum" operation, the operation "Max" is also commutative and associative. We select the maximum counts of several parts of the input(combiner), and then find the maximum count among them(reducer). They are the same operation.

###TopKWordCount.java
####How do Mappers and Reducers accept user-defined parameters?
We can set the accept parameter options during the job configuration:
```java
Configuration conf2 = new Configuration();
conf2.set("numResults", args[3]);
```
and retrieve those options in the configure() method of Mappers and Reducers classes:
```java	
Configuration conf = context.getConfiguration();
int kval = Integer.parseInt(conf.get("numResults"));
```
