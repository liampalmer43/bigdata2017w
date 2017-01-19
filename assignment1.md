### Assignment 2

## 1.)
Both the Pairs and Stripes implementation used 2 MapReduce Jobs.  In both cases, the first MapReduce job's Map simply tokenized lines of the input file and outputted (word1, 1) for every unique word in the first 40 words of a line, in addition to ("*", 1) for every line.  This allowed the line occurances for individual words and the total line count to be calculated easily in the first reducer.  This data was outputted to a directory that would later be read from in the second MapReduce job.

The Pairs' second MapReduce job's Map outputted (word1, word2) for every distinct pair of unique words in the first 40 characters of a line (of the same initial input file).  The Stripes' second MapReduce job's Map outputted (word, Map) with Map mapping every other distinct word in the first 40 characters of the line to 1.  In summary, both second phase maps are now counting the line occurances for pairs.  In the phase 2 reducer of both implementations, the line occurances for pairs of words is easily counted.  Additionally, the total line count and line count of single words in loaded from Hadoop Distributed Cache into a hash map in the setUp method.  This is the only additional data needed to calculate the PMI for every pair of words.

Pairs:    (Long, Text) -> M1 -> (Text, Int) -> R1 -> (Text, Int)

          (Long, Text) -> M2 -> (PairOfStrings, Int) -> R2 -> (PairOfStrings, PairOfFloatInt)

Stripes:  (Long, Text) -> M1 -> (Text, Int) -> R1 -> (Text, Int)

          (Long, Text) -> M2 -> (Text, HMapStIW) -> R2 -> (Text, HashMapWritable)

## 2.)
Run on linux.student.cs.uwaterloo.ca environment:

Pairs:

Job1 Finished in 6.031 seconds.
Job2 Finished in 47.914 seconds.

Total ~ 54s

Stripes:

Job1 Finished in 6.006 seconds.
Job2 Finished in 13.94 seconds.

Total ~ 20s

## 3.)
Run on linux.student.cs.uwaterloo.ca environment:

Pairs:

Job1 Finished in 7.999 seconds.
Job2 Finished in 57.914 seconds

Total ~ 66s

Stripes:

Job1 Finished in 7.974 seconds.
Job2 Finished in 15.929 seconds.

Total ~ 24s

## 4.)
77198 Distinct PMI Pairs

## 5.) 
Highest PMI: *(word1, word2) (PMI, co-occur count)*

(maine, anjou)  (3.6331422, 12)
(anjou, maine)  (3.6331422, 12)

In Shakespeare's Henry VI, Maine and Anjou are regions of France that are commonly discussed in the same context, thus resulting in a high PMI for these words.

Lowest PMI: *(word1, word2) (PMI, co-occur count)*

(thy, you)  (-1.5303967, 11)
(you, thy)  (-1.5303967, 11)

"Thy" means "Your", and the words you and your seem like unlikely combinations in Shakespeare, thus resulting in a low PMI for these words.

## 6.)
Highest PMI with tears: *(tears, word) (PMI, co-occur count)*

(tears, shed) (2.1117902, 15)

(tears, salt) (2.052812, 11)

(tears, eyes) (1.165167, 23)

Highest PMI with death: *(death, word) (PMI, co-occur count)*

(death, father's) (1.120252, 21)

(death, die)  (0.7541594, 18)

(death, life) (0.7381346, 31)

## 7.) 
->(Hockey, word) (PMI, co-occur count)

(hockey, defenceman)	(2.4030268, 147)

(hockey, winger)	(2.3863757, 185)

(hockey, goaltender)	(2.2434428, 198)

(hockey, ice)		(2.195185, 2002)

(hockey, nhl)		(1.9864639, 940)

## 8.)
->(data, word) (PMI, co-occur count)

(data, storage)		(1.9796829, 100)

(data, database)	(1.8992721, 97)

(data, disk)		(1.7935462, 67)

(data, stored)		(1.7868547, 65)

(data, processing)	(1.6476576, 57)
