lucene-c-boost
==============

#About
<p>
Optimized implementations of certain [Apache Lucene](http://lucene.apache.org) queries in C++ (via JNI) for anywhere from 0 to 6X speedup:

                    Task    QPS base      StdDev    QPS comp      StdDev                Pct diff
                  Fuzzy2      104.31      (5.4%)      125.54      (2.1%)   20.4% (  12% -   29%)
                  Fuzzy1       69.44      (4.2%)      112.71      (2.3%)   62.3% (  53% -   71%)
                Wildcard       20.94      (2.6%)       37.28      (8.0%)   78.1% (  65% -   91%)
                 LowTerm      276.75      (2.7%)      579.88      (3.1%)  109.5% ( 100% -  118%)
               OrHighLow       59.50      (6.6%)      146.91      (1.8%)  146.9% ( 129% -  166%)
                HighTerm       64.97      (3.0%)      178.87      (2.0%)  175.3% ( 165% -  185%)
                 MedTerm       55.92      (3.1%)      156.01      (1.9%)  179.0% ( 168% -  189%)
               OrHighMed       35.14      (6.6%)       99.69      (1.7%)  183.7% ( 164% -  205%)
              OrHighHigh       21.21      (6.9%)       61.20      (0.9%)  188.5% ( 169% -  210%)
                 Prefix3       21.88      (3.1%)       76.82      (8.6%)  251.2% ( 232% -  271%)
                  IntNRQ        7.27      (5.7%)       44.80      (5.9%)  516.2% ( 477% -  559%)

The gains come from 1) code specialization (creating dedicated code to execute exactly one kind of query, with nearly all abstractions removed), and 2) using C++ instead of Java.  It's not yet clear how much of the gains are due to each.

The code is fully decoupled from Lucene: it uses Java's reflection APIs to grab the necessary bits for each query.

This is NOT a port of Apache Lucene to C++!  Rather, it implements hardcoded C++ code to optimize certain queries.

This is a spinoff from [LUCENE-5049](https://issues.apache.org/jira/browse/LUCENE-5049).

<br>
#Usage
<p>
It's trivial to use; the only API is a public static method:

    NativeSearch.search(searcher, query, topN);

If the provided query matches then the optimized C++ code is used. Otherwise the normal Java implementation is used.

<br>
#Installation
<p>
Run python build.py then put dist/*.so on your dynamic library path, and dist/luceneCBoost.jar on your CLASSPATH.

<br>
#Limitations
<br>

  * Requires Lucene 4.3.0 or 4.3.1
  * Only tested on Linux / x86 CPU so far
  * Only sort-by-score is supported
  * Query must be either BooleanQuery with only SHOULD TermQuery clauses, or a single TermQuery, or rewrite to one of those (e.g., FuzzyQuery)
  * Must use the default 4.3 codec and Similarity
  * Must use the provided NativeMMapDirectory
  * This code is all very new and likely to have exciting bugs

<br>
#WARNING
Whenever native code is used from Java, if there are bugs (likely!), or API mis-use (such as closing a searcher while threads are still searcing against it), then the JVM will likely hit a SEGV and the OS will kill it.  You have been warned!