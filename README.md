#About
<p>
Optimized implementations of certain [Apache Lucene](http://lucene.apache.org) queries in C++ (via JNI) for anywhere from 0 to 7.8X speedup:

                    Task    QPS base      StdDev    QPS comp      StdDev                Pct diff
              AndHighLow      467.01      (0.5%)      294.88      (0.2%)  -36.9% ( -37% -  -36%)
                  Fuzzy1       62.97      (4.2%)       62.60      (2.0%)   -0.6% (  -6% -    5%)
                  Fuzzy2       25.51      (3.2%)       37.39      (2.0%)   46.6% (  40% -   53%)
              AndHighMed       50.43      (0.3%)      106.58      (0.6%)  111.4% ( 110% -  112%)
                 LowTerm      299.44      (0.4%)      684.10      (1.7%)  128.5% ( 125% -  130%)
            OrHighNotLow       45.77      (6.5%)      105.45      (0.1%)  130.4% ( 116% -  146%)
            OrHighNotMed       33.23      (6.1%)       87.89      (0.3%)  164.5% ( 148% -  182%)
           OrHighNotHigh        4.90      (6.6%)       14.08      (0.2%)  187.3% ( 169% -  207%)
                Wildcard       17.11      (0.4%)       51.35      (9.5%)  200.1% ( 189% -  210%)
               OrHighMed       18.19      (6.6%)       58.98      (1.4%)  224.2% ( 202% -  248%)
               OrHighLow       15.32      (6.8%)       50.39      (0.7%)  229.0% ( 207% -  253%)
              OrHighHigh        6.48      (6.4%)       21.36      (0.5%)  229.6% ( 209% -  252%)
                 MedTerm       69.58      (1.8%)      241.20      (2.3%)  246.7% ( 238% -  255%)
             AndHighHigh       21.98      (0.8%)       77.74      (1.3%)  253.8% ( 249% -  257%)
           OrNotHighHigh       12.90      (7.0%)       45.97      (0.6%)  256.5% ( 232% -  283%)
            OrNotHighMed       25.82      (7.5%)      114.32      (0.8%)  342.7% ( 310% -  379%)
                HighTerm       22.32      (1.8%)      108.60      (0.7%)  386.5% ( 377% -  396%)
                 Prefix3       10.13      (0.6%)       55.27      (1.8%)  445.3% ( 440% -  450%)
            OrNotHighLow       61.18      (7.4%)      346.80      (2.4%)  466.9% ( 425% -  514%)
                  IntNRQ        4.98      (0.5%)       38.62      (0.6%)  675.6% ( 671% -  680%)

The gains come from 1) code specialization (creating dedicated code to execute exactly one kind of query, with nearly all abstractions removed), and 2) using C++ instead of Java.  It's not yet clear how much of the gains are due to each.

The code is fully decoupled from Lucene: it uses Java's reflection APIs to grab the necessary bits for each query.

This is NOT a port of Apache Lucene to C++!  Rather, it implements hardcoded C++ code to optimize certain queries.  The optimizations are very *narrow*: they only apply for specific queries.

This is a spinoff from [LUCENE-5049](https://issues.apache.org/jira/browse/LUCENE-5049).  This project is very new and exploratory at this point.  Use at your own risk!  (But please provide feedback if you do).

<br>
#Usage
<p>
It's trivial to use; the only API is a public static method:

    NativeSearch.search(searcher, query, topN);

If the provided query matches then the optimized C++ code is used. Otherwise the normal Java implementation is used.

<br>
#Installation
<p>
If you're using Java 1.7 on Linux, just grab the binaries from dist/*.

Otherewise, run python build.py then put dist/*.so on your dynamic library path, and dist/luceneCBoost-SNAPSHOT.jar on your CLASSPATH.

<br>
#Limitations
<br>

  * Requires Lucene 4.3.x
  * Only tested on Linux / x86 CPU so far
  * Only sort-by-score is supported
  * Positional queries, and nested BooleanQuery (i.e., a query other than TermQuery as a clause inside BooleanQuery) and Filters are not optimized
  * Must use the default 4.3 codec and Similarity
  * Must use the provided NativeMMapDirectory
  * This code is all very new and likely to have exciting bugs

<br>
#WARNING
Whenever native code is used from Java, if there are bugs (likely!), or API mis-use (such as closing a searcher while threads are still searcing against it), then the JVM will likely hit a SEGV and the OS will kill it.  You have been warned!