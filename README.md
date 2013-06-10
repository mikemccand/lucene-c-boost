lucene-c-boost
==============

#About
<p>
Optimized implementations of certain [Apache Lucene](http://lucene.apache.org) queries in C++ (via JNI) for up to 2X - 3.5X speedup:

                    Task    QPS base      StdDev    QPS comp      StdDev                Pct diff
                  Fuzzy2       83.04      (5.4%)      100.58      (2.1%)   21.1% (  12% -   30%)
                  Fuzzy1      131.04     (10.6%)      159.59      (2.5%)   21.8% (   7% -   38%)
                 LowTerm      316.62      (0.4%)      725.67      (1.5%)  129.2% ( 126% -  131%)
                 MedTerm      105.21      (0.6%)      249.83      (1.1%)  137.5% ( 135% -  139%)
                HighTerm       29.19      (0.5%)       89.22      (0.7%)  205.7% ( 203% -  208%)
              OrHighHigh        8.28      (8.3%)       26.66      (1.2%)  221.9% ( 196% -  252%)
               OrHighLow       19.36      (8.9%)       63.58      (2.1%)  228.5% ( 199% -  263%)
               OrHighMed        4.35      (8.8%)       15.37      (1.5%)  253.4% ( 223% -  289%)

The gains come from 1) code specialization (creating dedicated code to execute exactly one kind of query, with nearly all abstractions removed), and 2) using C++ instead of Java.  It's not clear how much of the gains are due to each.

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
Run python3 build.py then put dist/*.so on your dynamic library path, and dist/luceneCBoost.jar on your CLASSPATH.

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
Whenever native code is used from Java, if there are bugs, or API mis-use (such as closing a searcher while threads are still searcing against it), then the JVM will hit a SEGV and the OS will kill it.  You have been warned!