package org.apache.lucene.search;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40LiveDocsFormat;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsFormat;
import org.apache.lucene.codecs.lucene42.Lucene42NormsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NativeMMapDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.InPlaceMergeSorter;

/** Uses JNI (C) code to execute a BooleanQuery.  Note that this
 *  class can currently only run in a very precise
 *  situation: searcher's leaves are all normal
 *  SegmentReaders, default postings format, collecting top
 *  scoring hits, only OR of TermQuery, using NativeMMapDirectory
 *  and default similarity. */

public class NativeSearch {

  static {
    System.loadLibrary("NativeSearch");
  }

  final IndexSearcher searcher;

  public NativeSearch(IndexSearcher searcher) {
    this.searcher = searcher;
  }

  private static native int searchSegmentBooleanQuery(
      // PQ holding top hits so far, pre-filled with sentinel
      // values: 
      int[] topDocIDs,
      float[] topScores,

      // Current segment's maxDoc
      int maxDoc,

      // Current segment's docBase
      int docBase,

      // Current segment's liveDocs, or null:
      byte[] liveDocBytes,
      
      // weightValue from each TermWeight:
      float[] termWeights,

      // Norms for the field (all TermQuery must be against a single field):
      byte[] norms,

      // nocommit silly to pass this once for each segment:
      // Cache, mapping byte norm -> float
      float[] normTable,

      // Coord factors from BQ:
      float[] coordFactors,

      // If the term has only one docID in this segment (it was "pulsed") then its set here, else -1:
      int[] singletonDocIDs,

      // docFreq of each term
      int[] docFreqs,
      
      // Offset in the .doc file where this term's docs+freqs begin:
      long[] docTermStartFPs,

      // Address in memory where .doc file is mapped:
      long docFileAddress);

  private static native int searchSegmentTermQuery(
      // PQ holding top hits so far, pre-filled with sentinel
      // values: 
      int[] topDocIDs,
      float[] topScores,

      // Current segment's maxDoc
      int maxDoc,

      // Current segment's docBase
      int docBase,

      // Current segment's liveDocs, or null:
      byte[] liveDocBytes,
      
      // weightValue for this TermQuery's TermWeight
      float termWeight,

      // Norms for the field (all TermQuery must be against a single field):
      byte[] norms,

      // nocommit silly to pass this once for each segment:
      // Cache, mapping byte norm -> float
      float[] normTable,

      // If the term has only one docID in this segment (it was "pulsed") then its set here, else -1:
      int singletonDocID,

      // docFreq
      int docFreq,
      
      // Offset in the .doc file where this term's docs+freqs begin:
      long docTermStartFP,

      // Address in memory where .doc file is mapped:
      long docFileAddress);

  public static TopDocs search(IndexSearcher searcher, Query query, int topN) throws IOException {

    query = searcher.rewrite(query);
    //System.out.println("after rewrite: " + query);

    try {
      return _search(searcher, query, topN);
    } catch (IllegalArgumentException iae) {
      return searcher.search(query, topN);
    }
  }

  private static TopDocs _search(IndexSearcher searcher, Query query, int topN) throws IOException {

    if (topN == 0) {
      throw new IllegalArgumentException("topN must be > 0; got: 0");
    }

    if (topN > searcher.getIndexReader().maxDoc()) {
      topN = searcher.getIndexReader().maxDoc();
    }

    if (query instanceof TermQuery) {
      return _searchTermQuery(searcher, (TermQuery) query, topN);
    } else if (query instanceof BooleanQuery) {
      return _searchBooleanQuery(searcher, (BooleanQuery) query, topN);
    } else {
      throw new IllegalArgumentException("rewritten query must be TermQuery or BooleanQuery; got: " + query);
    }
  }

  private static TopDocs _searchTermQuery(IndexSearcher searcher, TermQuery query, int topN) throws IOException {
    List<AtomicReaderContext> leaves = searcher.getIndexReader().leaves();
    Similarity sim = searcher.getSimilarity();

    if (!(sim instanceof DefaultSimilarity)) {
      throw new IllegalArgumentException("searcher.getSimilarity() must be DefaultSimilarity; got: " + sim);
    }
    Term term = query.getTerm();

    String field = term.field();
    String text = term.text();

    Weight w = searcher.createNormalizedWeight(query);

    float[] topScores = new float[topN+1];
    Arrays.fill(topScores, Float.MIN_VALUE);
    int[] topDocIDs = new int[topN+1];
    Arrays.fill(topDocIDs, Integer.MAX_VALUE);

    int totalHits = 0;

    for(int readerIDX=0;readerIDX<leaves.size();readerIDX++) {
      AtomicReaderContext ctx = leaves.get(readerIDX);
      if (!(ctx.reader() instanceof SegmentReader)) {
        throw new IllegalArgumentException("leaves must be SegmentReaders; got: " + ctx.reader());
      }
      SegmentReader reader = (SegmentReader) ctx.reader();
      if (!(reader.directory() instanceof NativeMMapDirectory)) {
        throw new IllegalArgumentException("directory must be a NativeMMapDirectory; got: " + reader.directory());
      }
      Codec codec = reader.getSegmentInfo().info.getCodec();

      FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
      if (fieldInfo.getIndexOptions() == FieldInfo.IndexOptions.DOCS_ONLY) {
        throw new IllegalArgumentException("field must be indexed with freqs; got: " + fieldInfo.getIndexOptions());
      }

      LiveDocsFormat ldf = codec.liveDocsFormat();
      if (!(ldf instanceof Lucene40LiveDocsFormat)) {
        throw new IllegalArgumentException("LiveDocsFormat must be Lucene40LiveDocsFormat; got: " + ldf);
      }

      PostingsFormat pf = codec.postingsFormat();
      if (pf instanceof PerFieldPostingsFormat) {
        pf = ((PerFieldPostingsFormat) pf).getPostingsFormatForField(field);
      }

      if (!(pf instanceof Lucene41PostingsFormat)) {
        throw new IllegalArgumentException("PostingsFormat for field=" + field + " must be Lucene41PostingsFormat; got: " + pf);
      }

      NormsFormat nf = codec.normsFormat();
      if (!(nf instanceof Lucene42NormsFormat)) {
        throw new IllegalArgumentException("NormsFormat for field=" + field + " must be Lucene42NormsFormat; got: " + nf);
      }

      NumericDocValues norms = reader.getNormValues(field);
      if (norms == null) {
        throw new IllegalArgumentException("field=" + field + " must not omit norms; got: no norms");
      }

      byte[] normBytes = getNormsBytes(norms);

      Bits liveDocs = reader.getLiveDocs();

      byte[] liveDocsBytes;
      if (liveDocs != null) {
        liveDocsBytes = getLiveDocsBits(liveDocs);
      } else {
        liveDocsBytes = null;
      }

      Scorer scorer = w.scorer(ctx, true, false, liveDocs);
      //System.out.println("search TQ:" + scorer);
      if (scorer != null) {

        float[] normTable = getNormTable();
        //System.out.println("SCORER: " + scorer);
        float termWeight = getTermWeight(scorer);

        int singletonDocID;

        long address;
        DocsEnum docsEnum = getDocsEnum(scorer);
        int docFreq = getDocFreq(docsEnum);
        long docTermStartFP = getDocTermStartFP(docsEnum);
          
        if (docFreq > 1) {
          IndexInput docIn = getDocIn(docsEnum);
          address = getMMapAddress(docIn);
          singletonDocID = -1;
        } else {
          // Pulsed
          address = 0;
          singletonDocID = getSingletonDocID(docsEnum);
          assert singletonDocID >= 0;
        }

        totalHits += searchSegmentTermQuery(topDocIDs,
                                            topScores,
                                            reader.maxDoc(),
                                            ctx.docBase,
                                            liveDocsBytes,
                                            termWeight,
                                            normBytes,
                                            normTable,
                                            singletonDocID,
                                            docFreq,
                                            docTermStartFP,
                                            address);
      }
    }

    //System.out.println("totalHits=" + totalHits);

    ScoreDoc[] scoreDocs = new ScoreDoc[Math.min(totalHits, topN)];

    int heapSize = topN;

    for(int i=0;i<topN-scoreDocs.length;i++) {
      topScores[1] = topScores[heapSize];
      topDocIDs[1] = topDocIDs[heapSize];
      heapSize--;
      downHeap(heapSize, topDocIDs, topScores);
    }

    for(int i=scoreDocs.length-1;i>=0;i--) {
      scoreDocs[i] = new ScoreDoc(topDocIDs[1], topScores[1]);
      topScores[1] = topScores[heapSize];
      topDocIDs[1] = topDocIDs[heapSize];
      //System.out.println("  topDocs[" + i + "]=" + scoreDocs[i].doc);
      heapSize--;
      downHeap(heapSize, topDocIDs, topScores);
    }

    float maxScore;
    if (scoreDocs.length > 0) {
      maxScore = scoreDocs[0].score;
    } else {
      maxScore = Float.NaN;
    }

    return new TopDocs(totalHits, scoreDocs, maxScore);
  }

  private static TopDocs _searchBooleanQuery(IndexSearcher searcher, BooleanQuery query, int topN) throws IOException {

    List<AtomicReaderContext> leaves = searcher.getIndexReader().leaves();
    Similarity sim = searcher.getSimilarity();

    if (!(sim instanceof DefaultSimilarity)) {
      throw new IllegalArgumentException("searcher.getSimilarity() must be DefaultSimilarity; got: " + sim);
    }

    if (query.getMinimumNumberShouldMatch() > 1) {
      throw new IllegalArgumentException("can only handle minimumNumberShouldMatch==0; got: " + query.getMinimumNumberShouldMatch());
    }

    boolean coordDisabled = query.isCoordDisabled();

    BooleanClause[] clauses = query.getClauses();
    if (clauses.length == 0) {
      throw new IllegalArgumentException("query must have at least one BooleanClause; got: none");
    }

    String field = null;
    String[] terms = new String[clauses.length];
    for(int i=0;i<clauses.length;i++) {
      BooleanClause clause = clauses[i];
      if (clause.getOccur() != BooleanClause.Occur.SHOULD) {
        throw new IllegalArgumentException("only Occur.SHOULD supported; got: " + clause.getOccur());
      }
      if (!(clause.getQuery() instanceof TermQuery)) {
        throw new IllegalArgumentException("sub-queries must be TermQuery; got: " + clause.getQuery());
      }
      TermQuery tq = (TermQuery) clause.getQuery();
      Term term = tq.getTerm();
      if (i == 0) {
        field = term.field();
      } else if (!field.equals(term.field())) {
        throw new IllegalArgumentException("all sub-queries must be TermQuery against the same field; got both field=" + field + " and field=" + term.field());
      }
      terms[i] = term.text();
    }

    Weight w = searcher.createNormalizedWeight(query);

    List<Weight> subWeights = getBooleanSubWeights(w);

    float[] topScores = new float[topN+1];
    Arrays.fill(topScores, Float.MIN_VALUE);
    int[] topDocIDs = new int[topN+1];
    Arrays.fill(topDocIDs, Integer.MAX_VALUE);
    int totalHits = 0;

    for(int readerIDX=0;readerIDX<leaves.size();readerIDX++) {
      AtomicReaderContext ctx = leaves.get(readerIDX);
      if (!(ctx.reader() instanceof SegmentReader)) {
        throw new IllegalArgumentException("leaves must be SegmentReaders; got: " + ctx.reader());
      }
      SegmentReader reader = (SegmentReader) ctx.reader();
      if (!(reader.directory() instanceof NativeMMapDirectory)) {
        throw new IllegalArgumentException("directory must be a NativeMMapDirectory; got: " + reader.directory());
      }
      Codec codec = reader.getSegmentInfo().info.getCodec();

      FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
      if (fieldInfo.getIndexOptions() == FieldInfo.IndexOptions.DOCS_ONLY) {
        throw new IllegalArgumentException("field must be indexed with freqs; got: " + fieldInfo.getIndexOptions());
      }

      LiveDocsFormat ldf = codec.liveDocsFormat();
      if (!(ldf instanceof Lucene40LiveDocsFormat)) {
        throw new IllegalArgumentException("LiveDocsFormat must be Lucene40LiveDocsFormat; got: " + ldf);
      }

      PostingsFormat pf = codec.postingsFormat();
      if (pf instanceof PerFieldPostingsFormat) {
        pf = ((PerFieldPostingsFormat) pf).getPostingsFormatForField(field);
      }

      if (!(pf instanceof Lucene41PostingsFormat)) {
        throw new IllegalArgumentException("PostingsFormat for field=" + field + " must be Lucene41PostingsFormat; got: " + pf);
      }

      NormsFormat nf = codec.normsFormat();
      if (!(nf instanceof Lucene42NormsFormat)) {
        throw new IllegalArgumentException("NormsFormat for field=" + field + " must be Lucene42NormsFormat; got: " + nf);
      }

      NumericDocValues norms = reader.getNormValues(field);
      if (norms == null) {
        throw new IllegalArgumentException("field=" + field + " must not omit norms; got: no norms");
      }

      byte[] normBytes = getNormsBytes(norms);

      Bits liveDocs = reader.getLiveDocs();

      byte[] liveDocsBytes;
      if (liveDocs != null) {
        liveDocsBytes = getLiveDocsBits(liveDocs);
      } else {
        liveDocsBytes = null;
      }

      List<Scorer> scorers = new ArrayList<Scorer>();
      for(int i=0;i<terms.length;i++) {
        Scorer scorer = subWeights.get(i).scorer(ctx, true, false, liveDocs);
        if (scorer != null) {
          scorers.add(scorer);
        }
      }

      if (!scorers.isEmpty()) {

        float[] coordFactors = new float[scorers.size()+1];
        for(int i=0;i<coordFactors.length;i++) {
          float f;
          if (coordDisabled) {
            f = 1.0f;
          } else if (scorers.size() == 1) {
            f = 1.0f;
          } else {
            f = sim.coord(i, scorers.size());
          }
          coordFactors[i] = f;
        }

        float[] normTable = getNormTable();
        final float[] termWeights = new float[scorers.size()];
        final int[] singletonDocIDs = new int[scorers.size()];
        final int[] docFreqs = new int[scorers.size()];
        final long[] docTermStartFPs = new long[scorers.size()];
        long address = 0;
        for(int i=0;i<scorers.size();i++) {
          Scorer scorer = scorers.get(i);
          termWeights[i] = getTermWeight(scorer);
          DocsEnum docsEnum = getDocsEnum(scorer);
          docFreqs[i] = getDocFreq(docsEnum);
          docTermStartFPs[i] = getDocTermStartFP(docsEnum);
          
          if (docFreqs[i] > 1) {
            IndexInput docIn = getDocIn(docsEnum);
            if (address == 0) {
              address = getMMapAddress(docIn);
            }
            singletonDocIDs[i] = -1;
          } else {
            // Pulsed
            singletonDocIDs[i] = getSingletonDocID(docsEnum);
            assert singletonDocIDs[i] >= 0;
          }
        }

        // Sort by descending docFreq: we do this because
        // the first scorer is handled separately (saves an
        // if inside the inner loop):
        new InPlaceMergeSorter() {
          @Override
          protected int compare(int i, int j) {
            return docFreqs[j] - docFreqs[i];
          }

          @Override
          protected void swap(int i, int j) {
            int x = docFreqs[i];
            docFreqs[i] = docFreqs[j];
            docFreqs[j] = x;

            x = singletonDocIDs[i];
            singletonDocIDs[i] = singletonDocIDs[j];
            singletonDocIDs[j] = x;

            long y = docTermStartFPs[i];
            docTermStartFPs[i] = docTermStartFPs[j];
            docTermStartFPs[j] = y;

            float z = termWeights[i];
            termWeights[i] = termWeights[j];
            termWeights[j] = z;
          }
        }.sort(0, scorers.size()-1);
        
        totalHits += searchSegmentBooleanQuery(topDocIDs,
                                               topScores,
                                               reader.maxDoc(),
                                               ctx.docBase,
                                               liveDocsBytes,
                                               termWeights,
                                               normBytes,
                                               normTable,
                                               coordFactors,
                                               singletonDocIDs,
                                               docFreqs,
                                               docTermStartFPs,
                                               address);
      }
    }

    //System.out.println("totalHits=" + totalHits);

    ScoreDoc[] scoreDocs = new ScoreDoc[Math.min(totalHits, topN)];

    int heapSize = topN;

    for(int i=0;i<topN-scoreDocs.length;i++) {
      topScores[1] = topScores[heapSize];
      topDocIDs[1] = topDocIDs[heapSize];
      heapSize--;
      downHeap(heapSize, topDocIDs, topScores);
    }

    for(int i=scoreDocs.length-1;i>=0;i--) {
      scoreDocs[i] = new ScoreDoc(topDocIDs[1], topScores[1]);
      topScores[1] = topScores[heapSize];
      topDocIDs[1] = topDocIDs[heapSize];
      //System.out.println("  topDocs[" + i + "]=" + scoreDocs[i].doc);
      heapSize--;
      downHeap(heapSize, topDocIDs, topScores);
    }

    float maxScore;
    if (scoreDocs.length > 0) {
      maxScore = scoreDocs[0].score;
    } else {
      maxScore = Float.NaN;
    }

    return new TopDocs(totalHits, scoreDocs, maxScore);
  }

  private static boolean lessThan(int docID1, float score1, int docID2, float score2) {
    if (score1 < score2) {
      return true;
    } else if (score1 > score2) {
      return false;
    } else {
      if (docID1 > docID2) {
        return true;
      } else {
        return false;
      }
    }
  }

  private static void downHeap(int heapSize, int[] topDocIDs, float[] topScores) {
    int i = 1;
    // save top node
    int savDocID = topDocIDs[i];
    float savScore = topScores[i];
    int j = i << 1;            // find smaller child
    int k = j + 1;
    if (k <= heapSize && lessThan(topDocIDs[k], topScores[k], topDocIDs[j], topScores[j])) {
      j = k;
    }
    while (j <= heapSize && lessThan(topDocIDs[j], topScores[j], savDocID, savScore)) {
      // shift up child
      topDocIDs[i] = topDocIDs[j];
      topScores[i] = topScores[j];
      i = j;
      j = i << 1;
      k = j + 1;
      if (k <= heapSize && lessThan(topDocIDs[k], topScores[k], topDocIDs[j], topScores[j])) {
        j = k;
      }
    }
    // install saved node
    topDocIDs[i] = savDocID;
    topScores[i] = savScore;
  }

  // nocommit we can move most of the reflection lookups to
  // static:

  private static float[] getNormTable() {
    try {
      Class<?> x = Class.forName("org.apache.lucene.search.similarities.TFIDFSimilarity");
      Field f = x.getDeclaredField("NORM_TABLE");
      f.setAccessible(true);
      return (float[]) f.get(x);
    } catch (Exception e) {
      throw new IllegalStateException("failed to access NORM_TABLE", e);
    }
  }

  private static long getMMapAddress(IndexInput in) {
    try {
      Class<?> x = Class.forName("org.apache.lucene.store.NativeMMapDirectory$NativeMMapIndexInput");
      Field f = x.getDeclaredField("address");
      f.setAccessible(true);
      return f.getLong(in);
    } catch (Exception e) {
      throw new IllegalStateException("failed to access ByteBuffer addresses via reflection", e);
    }
  }

  private static float getTermWeight(Scorer scorer) {
    try {
      Class<?> x = Class.forName("org.apache.lucene.search.TermScorer");
      Field f = x.getDeclaredField("docScorer");
      f.setAccessible(true);
      Object o = f.get(scorer);
      Class<?> y = Class.forName("org.apache.lucene.search.similarities.TFIDFSimilarity$TFIDFSimScorer");
      Field weightsField = y.getDeclaredField("weightValue");
      weightsField.setAccessible(true);
      return weightsField.getFloat(o);
    } catch (Exception e) {
      throw new IllegalStateException("failed to access TFIDFSimilarity via reflection", e);
    }
  }

  private static byte[] getLiveDocsBits(Bits bits) {
    try {
      Class<?> x = Class.forName("org.apache.lucene.codecs.lucene40.BitVector");
      Field f = x.getDeclaredField("bits");
      f.setAccessible(true);
      return (byte[]) f.get(bits);
    } catch (Exception e) {
      throw new IllegalStateException("failed to access TermScorer.docsEnum via reflection", e);
    }
  }

  private static DocsEnum getDocsEnum(Scorer scorer) {
    try {
      Class<?> x = Class.forName("org.apache.lucene.search.TermScorer");
      Field f = x.getDeclaredField("docsEnum");
      f.setAccessible(true);
      return (DocsEnum) f.get(scorer);
    } catch (Exception e) {
      throw new IllegalStateException("failed to access TermScorer.docsEnum via reflection", e);
    }
  }

  private static int getDocFreq(DocsEnum docsEnum) {
    try {
      Class<?> x = Class.forName("org.apache.lucene.codecs.lucene41.Lucene41PostingsReader$BlockDocsEnum");
      Field f = x.getDeclaredField("docFreq");
      f.setAccessible(true);
      return f.getInt(docsEnum);
    } catch (Exception e) {
      throw new IllegalStateException("failed to access docFreq via reflection", e);
    }
  }

  private static IndexInput getDocIn(DocsEnum docsEnum) {
    try {
      final Class<?> x = Class.forName("org.apache.lucene.codecs.lucene41.Lucene41PostingsReader$BlockDocsEnum");
      final Field f = x.getDeclaredField("startDocIn");
      f.setAccessible(true);
      return (IndexInput) f.get(docsEnum);
    } catch (Exception e) {
      throw new IllegalStateException("failed to access docFreq via reflection", e);
    }
  }

  private static long getDocTermStartFP(DocsEnum docsEnum) {
    try {
      final Class<?> x = Class.forName("org.apache.lucene.codecs.lucene41.Lucene41PostingsReader$BlockDocsEnum");
      final Field f = x.getDeclaredField("docTermStartFP");
      f.setAccessible(true);
      return f.getLong(docsEnum);
    } catch (Exception e) {
      throw new IllegalStateException("failed to access docFreq via reflection", e);
    }
  }

  private static int getSingletonDocID(DocsEnum docsEnum) {
    try {
      final Class<?> x = Class.forName("org.apache.lucene.codecs.lucene41.Lucene41PostingsReader$BlockDocsEnum");
      final Field f = x.getDeclaredField("singletonDocID");
      f.setAccessible(true);
      return f.getInt(docsEnum);
    } catch (Exception e) {
      throw new IllegalStateException("failed to access docFreq via reflection", e);
    }
  }

  @SuppressWarnings("unchecked")
  private static List<Weight> getBooleanSubWeights(Weight w) {
    try {
      final Class<?> x = Class.forName("org.apache.lucene.search.BooleanQuery$BooleanWeight");
      final Field weightsField = x.getDeclaredField("weights");
      weightsField.setAccessible(true);
      return (List<Weight>) weightsField.get(w);
    } catch (Exception e) {
      throw new IllegalStateException("failed to access BooleanWeight.weights via reflection", e);
    }
  }

  private static byte[] getNormsBytes(NumericDocValues norms) {
    try {
      final Class<?> x = Class.forName("org.apache.lucene.codecs.lucene42.Lucene42DocValuesProducer$3");
      final Field bytesField = x.getDeclaredField("val$bytes");
      bytesField.setAccessible(true);
      return (byte[]) bytesField.get(norms);
    } catch (Exception e) {
      throw new IllegalStateException("failed to access norm byte[] via reflection", e);
    }
  }
}
