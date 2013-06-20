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

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40LiveDocsFormat;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsFormat;
import org.apache.lucene.codecs.lucene42.Lucene42NormsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NativeMMapDirectory;
import org.apache.lucene.util.*;

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
      byte[] liveDocsBytes,
      
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

      long[] totalTermFreqs,

      // docFreq of each term
      int[] docFreqs,
      
      // Offset in the .doc file where this term's docs+freqs begin:
      long[] docTermStartFPs,

      // Address in memory where .doc file is mapped:
      long docFileAddress,

      int numMustNot,

      int numMust);

  private static native int searchSegmentExactPhraseQuery(
      // PQ holding top hits so far, pre-filled with sentinel
      // values: 
      int[] topDocIDs,
      float[] topScores,

      // Current segment's maxDoc
      int maxDoc,

      // Current segment's docBase
      int docBase,

      // Current segment's liveDocs, or null:
      byte[] liveDocsBytes,
      
      // weightValue from each TermWeight:
      float termWeight,

      // Norms for the field (all TermQuery must be against a single field):
      byte[] norms,

      // nocommit silly to pass this once for each segment:
      // Cache, mapping byte norm -> float
      float[] normTable,

      // If the term has only one docID in this segment (it was "pulsed") then its set here, else -1:
      int[] singletonDocIDs,

      long[] totalTermFreqs,

      // docFreq of each term
      int[] docFreqs,
      
      // Offset in the .doc file where this term's docs+freqs begin:
      long[] docTermStartFPs,

      // Offset in the .pos file where this term's positions begin:
      long[] posTermStartFPs,

      // Offset of each term in the phrase (first term is 0,
      // 2nd is -1, ...):
      int[] posOffsets,

      // Address in memory where .doc file is mapped:
      long docFileAddress,

      // Address in memory where .pos file is mapped:
      long posFileAddress,

      boolean indexHasPayloads,

      boolean indexHasOffsets);

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
      byte[] liveDocsBytes,

      boolean docsOnly,
      
      // weightValue for this TermQuery's TermWeight
      float termWeight,

      // Norms for the field (all TermQuery must be against a single field):
      byte[] norms,

      // nocommit silly to pass this once for each segment:
      // Cache, mapping byte norm -> float
      float[] normTable,

      // If the term has only one docID in this segment (it was "pulsed") then its set here, else -1:
      int singletonDocID,

      long totalTermFreq,

      // docFreq
      int docFreq,
      
      // Offset in the .doc file where this term's docs+freqs begin:
      long docTermStartFP,

      // Address in memory where .doc file is mapped:
      long docFileAddress);

  private static native void fillMultiTermFilter(
      long[] bits,

      byte[] liveDocsBytes,

      long address,

      long[] termStatsArray,

      boolean docsOnly);

  /** Runs the search, using optimized C++ code when
   *  possible, but otherwise falling back on
   *  IndexSearcher. */
  public static TopDocs search(IndexSearcher searcher, Query query, int topN) throws IOException {
    //System.out.println("NATIVE: query in: " + query);
    query = searcher.rewrite(query);
    //System.out.println("NATIVE: after rewrite: " + query);

    try {
      TopDocs hits = _search(searcher, query, topN);
      //System.out.println("NATIVE: " + hits.totalHits + " hits");
      return hits;
    } catch (IllegalArgumentException iae) {
      //System.out.println("NATIVE: skip: " + iae);
      return searcher.search(query, null, topN);
    }
  }

  /** Runs the search using only optimized C++ code; if the
   *  search cannot be optimized then {@code
   *  IllegalArgumentException} is thrown with the reason. */
  public static TopDocs searchNative(IndexSearcher searcher, Query query, int topN) throws IOException {
    //System.out.println("NATIVE: query in=" + query);
    query = searcher.rewrite(query);
    //System.out.println("NATIVE: after rewrite: " + query + "; " + query.getClass());
    return _search(searcher, query, topN);
  }
  
  private static class SegmentState {
    SegmentReader reader;
    byte[] normBytes;
    byte[] liveDocsBytes;
    boolean skip;
    boolean docsOnly;
    Bits liveDocs;
    int maxDoc;

    public SegmentState(AtomicReaderContext ctx, String field) throws IOException {
      if (!(ctx.reader() instanceof SegmentReader)) {
        throw new IllegalArgumentException("leaves must be SegmentReaders; got: " + ctx.reader());
      }
      reader = (SegmentReader) ctx.reader();
      maxDoc = reader.maxDoc();
      Directory dir = unwrap(reader.directory());
      if (!(dir instanceof NativeMMapDirectory)) {
        throw new IllegalArgumentException("directory must be a NativeMMapDirectory; got: " + reader.directory());
      }
      Codec codec = reader.getSegmentInfo().info.getCodec();

      FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
      if (fieldInfo == null) {
        // Field never appeared in this segment so no docs
        // will match:
        skip = true;
        return;
      }
      skip = false;
      docsOnly = fieldInfo.getIndexOptions() == FieldInfo.IndexOptions.DOCS_ONLY;

      LiveDocsFormat ldf = codec.liveDocsFormat();
      if (!(ldf instanceof Lucene40LiveDocsFormat)) {
        throw new IllegalArgumentException("LiveDocsFormat must be Lucene40LiveDocsFormat; got: " + ldf);
      }

      NormsFormat nf = codec.normsFormat();
      if (!(nf instanceof Lucene42NormsFormat)) {
        throw new IllegalArgumentException("NormsFormat for field=" + field + " must be Lucene42NormsFormat; got: " + nf);
      }

      NumericDocValues norms = reader.getNormValues(field);
      if (norms == null) {
        normBytes = null;
      } else {
        normBytes = getNormsBytes(norms);
      }

      liveDocs = reader.getLiveDocs();

      if (liveDocs != null) {
        liveDocsBytes = getLiveDocsBits(liveDocs);
      }
    }
  }

  private static TopDocs _search(IndexSearcher searcher, Query query, int topN) throws IOException {

    if (topN == 0) {
      throw new IllegalArgumentException("topN must be > 0; got: 0");
    }

    if (topN > searcher.getIndexReader().maxDoc()) {
      topN = searcher.getIndexReader().maxDoc();
    }

    float constantScore = -1.0f;
    if (query instanceof ConstantScoreQuery) {
      ConstantScoreQuery csq = (ConstantScoreQuery) query;
      Query other = csq.getQuery();
      // Must null check because CSQ can also wrap a filter:
      if (other != null) {
        constantScore = csq.getBoost();
        query = other;
        //System.out.println("unwrap csq to " + query.getClass());
      } else {
        Filter f = csq.getFilter();
        if (f instanceof MultiTermQueryWrapperFilter) {
          //System.out.println("NATIVE: mtq filter " + f);
          return _searchMTQFilter(searcher, (MultiTermQueryWrapperFilter) f, topN, csq.getBoost());
        }
      }
    }

    // System.out.println("NATIVE: search " + query);

    if (query instanceof TermQuery) {
      return _searchTermQuery(searcher, (TermQuery) query, topN, constantScore);
    } else if (query instanceof PhraseQuery) {
      return _searchPhraseQuery(searcher, (PhraseQuery) query, topN, constantScore);
    } else if (query instanceof BooleanQuery) {
      return _searchBooleanQuery(searcher, (BooleanQuery) query, topN, constantScore);
    } else {
      throw new IllegalArgumentException("rewritten query must be TermQuery, BooleanQuery or PhraseQuery; got: " + query.getClass());
    }
  }

  private static TopDocs _searchMTQFilter(IndexSearcher searcher, MultiTermQueryWrapperFilter filter, int topN, float constantScore) throws IOException {
    //System.out.println("MTQ search");
    List<AtomicReaderContext> leaves = searcher.getIndexReader().leaves();
    Similarity sim = searcher.getSimilarity();

    MultiTermQuery query = getMultiTermQueryWrapperFilterQuery(filter);

    if (!(sim instanceof DefaultSimilarity)) {
      throw new IllegalArgumentException("searcher.getSimilarity() must be DefaultSimilarity; got: " + sim);
    }

    String field = filter.getField();

    int[] topDocIDs = new int[topN+1];
    Arrays.fill(topDocIDs, Integer.MAX_VALUE);

    List<ScoreDoc> scoreDocs = new ArrayList<ScoreDoc>();
    int totalHits = 0;
    for(int readerIDX=0;readerIDX<leaves.size();readerIDX++) {
      AtomicReaderContext ctx = leaves.get(readerIDX);
      SegmentState state = new SegmentState(ctx, field);
      if (state.skip) {
        continue;
      }

      Fields fields = state.reader.fields();
      if (fields == null) {
        continue;
      }

      Terms terms = fields.terms(field);
      if (terms == null) {
        continue;
      }

      List<Long> termStats = new ArrayList<Long>();

      TermsEnum termsEnum = query.getTermsEnum(terms);
      if (termsEnum.next() == null) {
        continue;
      }

      DocsEnum docsEnum = termsEnum.docs(null, null);

      // fill into a FixedBitSet
      FixedBitSet bitSet = new FixedBitSet(state.maxDoc);

      if (termsEnum instanceof FilteredTermsEnum) {
        TermsEnum wrappedTermsEnum = getActualTEnum(termsEnum);
        //System.out.println("wrapped TE=" + wrappedTermsEnum);

        do {
          BlockTermState termState = getTermState(wrappedTermsEnum);
          if (termState.docFreq == 1) {
            // Pulsed
            int docID = getSingletonDocID(termState);
            if (state.liveDocs == null || state.liveDocs.get(docID)) {
              bitSet.set(docID);
              //System.out.println("term: " + termsEnum.term() + " pulsed: docID=" + docID);
            }
          } else {
            termStats.add((long) termState.docFreq);
            termStats.add(getDocTermStartFP(termState));
            //System.out.println("term: " + termsEnum.term() + " docFreq=" + termState.docFreq + " startFP=" + getDocTermStartFP(termState));
          }
        } while (termsEnum.next() != null);
      } else if (blockTreeIntersectEnum.isInstance(termsEnum)) {
        do {
          BlockTermState termState = getIntersectTermState(termsEnum);
          if (termState.docFreq == 1) {
            // Pulsed
            int docID = getSingletonDocID(termState);
            if (state.liveDocs == null || state.liveDocs.get(docID)) {
              bitSet.set(docID);
            }
          } else {
            termStats.add((long) termState.docFreq);
            termStats.add(getDocTermStartFP(termState));
            //System.out.println("term: " + termsEnum.term().utf8ToString() + " docFreq=" + termState.docFreq + " startFP=" + getDocTermStartFP(termState));
          }
        } while (termsEnum.next() != null);
      } else {
        throw new IllegalArgumentException("can only handle FilteredTermsEnum; got " + termsEnum);
      }

      if (!termStats.isEmpty()) {
        long[] bitSetBits = (long[]) getFieldObject(bitSet, "org.apache.lucene.util.FixedBitSet", "bits");
        long[] termStatsArray = new long[termStats.size()];
        for(int i=0;i<termStatsArray.length;i++) { 
          termStatsArray[i] = termStats.get(i);
        }
        IndexInput docIn = getDocIn(docsEnum);
        //System.out.println(termStatsArray.length + " terms to MTQ");
        fillMultiTermFilter(bitSetBits, state.liveDocsBytes, getMMapAddress(docIn), termStatsArray, state.docsOnly);
      }

      if (scoreDocs.size() < topN) {
        int docID = bitSet.nextSetBit(0);
        if (docID != -1) {
          while (true) {
            //System.out.println("collect docID=" + ctx.docBase + " + " + docID);
            scoreDocs.add(new ScoreDoc(ctx.docBase + docID, constantScore));
            if (scoreDocs.size() == topN) {
              break;
            }
            if (docID == state.maxDoc-1) {
              break;
            }
            docID = bitSet.nextSetBit(docID+1);
            if (docID == -1) {
              break;
            }
          }
        }
      }
      totalHits += bitSet.cardinality();
      // TODO: not until we add needsTotalHitCount
      /*
        if (scoreDocs.size() == topN) {
        break;
        }
      */
    }

    return new TopDocs(totalHits, scoreDocs.toArray(new ScoreDoc[scoreDocs.size()]), scoreDocs.isEmpty() ? Float.NaN : constantScore);
  }

  static final Field blockTermStateDocStartFPField;
  static final Field blockTermStateSingletonDocIDField;
  static final Field blockTreeCurrentFrameField;
  static final Method decodeMetaDataMethod;
  static final Method intersectDecodeMetaDataMethod;
  static final Field frameStateField;
  static final Class blockTreeIntersectEnum;
  static final Field blockTreeIntersectCurrentFrameField;
  static final Field intersectFrameStateField;
  static final Field blockDocsEnumSingletonDocIDField;
  static final Field blockDocsEnumTotalTermFreqField;

  static {
    try {
      Class<?> x;
      blockTermStateDocStartFPField = getField("org.apache.lucene.codecs.lucene41.Lucene41PostingsReader$IntBlockTermState",
                                               "docStartFP");

      blockTermStateSingletonDocIDField = getField("org.apache.lucene.codecs.lucene41.Lucene41PostingsReader$IntBlockTermState",
                                               "singletonDocID");

      blockTreeCurrentFrameField = getField("org.apache.lucene.codecs.BlockTreeTermsReader$FieldReader$SegmentTermsEnum",
                                            "currentFrame");

      decodeMetaDataMethod = getMethod("org.apache.lucene.codecs.BlockTreeTermsReader$FieldReader$SegmentTermsEnum$Frame",
                                       "decodeMetaData");
      frameStateField = getField("org.apache.lucene.codecs.BlockTreeTermsReader$FieldReader$SegmentTermsEnum$Frame",
                                 "state");

      blockTreeIntersectEnum = Class.forName("org.apache.lucene.codecs.BlockTreeTermsReader$FieldReader$IntersectEnum");
      blockTreeIntersectCurrentFrameField = getField("org.apache.lucene.codecs.BlockTreeTermsReader$FieldReader$IntersectEnum",
                                                     "currentFrame");

      intersectDecodeMetaDataMethod = getMethod("org.apache.lucene.codecs.BlockTreeTermsReader$FieldReader$IntersectEnum$Frame",
                                                "decodeMetaData");
      intersectFrameStateField = getField("org.apache.lucene.codecs.BlockTreeTermsReader$FieldReader$IntersectEnum$Frame",
                                          "termState");

      blockDocsEnumSingletonDocIDField = getField("org.apache.lucene.codecs.lucene41.Lucene41PostingsReader$BlockDocsEnum",
                                                  "singletonDocID");

      blockDocsEnumTotalTermFreqField = getField("org.apache.lucene.codecs.lucene41.Lucene41PostingsReader$BlockDocsEnum",
                                                 "totalTermFreq");
    } catch (Exception e) {
      throw new IllegalStateException("reflection failed", e);
    }
  }

  private static long getDocTermStartFP(BlockTermState state) {
    try {
      return blockTermStateDocStartFPField.getLong(state);
    } catch (Exception e) {
      throw new IllegalStateException("reflection failed");
    }
  }

  private static int getSingletonDocID(BlockTermState state) {
    try {
      return blockTermStateSingletonDocIDField.getInt(state);
    } catch (Exception e) {
      throw new IllegalStateException("reflection failed", e);
    }
  }

  private static BlockTermState getTermState(TermsEnum termsEnum) {
    //System.out.println("TE: " + termsEnum);
    try {
      Object o = blockTreeCurrentFrameField.get(termsEnum);
      decodeMetaDataMethod.invoke(o);
      return (BlockTermState) frameStateField.get(o);
    } catch (Exception e) {
      throw new IllegalStateException("reflection failed", e);
    }
  }

  private static BlockTermState getIntersectTermState(TermsEnum termsEnum) {
    //System.out.println("TE: " + termsEnum);
    try {
      Object o = blockTreeIntersectCurrentFrameField.get(termsEnum);
      intersectDecodeMetaDataMethod.invoke(o);
      return (BlockTermState) intersectFrameStateField.get(o);
    } catch (Exception e) {
      throw new IllegalStateException("reflection failed", e);
    }
  }

  private static Object getFieldObject(Object o, String className, String fieldName) {
    try {
      Class<?> x = Class.forName(className);
      Field f = x.getDeclaredField(fieldName);
      f.setAccessible(true);
      return f.get(o);
    } catch (Exception e) {
      throw new RuntimeException("failed to get field=" + fieldName + " from class=" + className + " object=" + o, e);
    }
  }

  private static Field getField(String className, String fieldName) {
    try {
      Class<?> x = Class.forName(className);
      Field f = x.getDeclaredField(fieldName);
      f.setAccessible(true);
      return f;
    } catch (Exception e) {
      throw new RuntimeException("failed to get field=" + fieldName + " from class=" + className, e);
    }
  }

  private static Method getMethod(String className, String methodName) {
    try {
      Class<?> x = Class.forName(className);
      Method f = x.getDeclaredMethod(methodName);
      f.setAccessible(true);
      return f;
    } catch (Exception e) {
      throw new RuntimeException("failed to get method=" + methodName + " from class=" + className, e);
    }
  }

  private static int getIntField(Object o, String className, String fieldName) {
    try {
      Class<?> x = Class.forName(className);
      Field f = x.getDeclaredField(fieldName);
      f.setAccessible(true);
      return f.getInt(o);
    } catch (Exception e) {
      throw new RuntimeException("failed to get field=" + fieldName + " from class=" + className + " object=" + o, e);
    }
  }

  private static long getLongField(Object o, String className, String fieldName) {
    try {
      Class<?> x = Class.forName(className);
      Field f = x.getDeclaredField(fieldName);
      f.setAccessible(true);
      return f.getLong(o);
    } catch (Exception e) {
      throw new RuntimeException("failed to get field=" + fieldName + " from class=" + className + " object=" + o, e);
    }
  }

  private static MultiTermQuery getMultiTermQueryWrapperFilterQuery(MultiTermQueryWrapperFilter filter) {
    try {
      Class<?> x = Class.forName("org.apache.lucene.search.MultiTermQueryWrapperFilter");
      Field f = x.getDeclaredField("query");
      f.setAccessible(true);
      return (MultiTermQuery) f.get(filter);
    } catch (Exception e) {
      throw new RuntimeException("failed to extract MTQWF query", e);
    }
  }

  private static TermsEnum getActualTEnum(TermsEnum termsEnum) {
    try {
      Class<?> x = Class.forName("org.apache.lucene.index.FilteredTermsEnum");
      Field f = x.getDeclaredField("tenum");
      f.setAccessible(true);
      return (TermsEnum) f.get(termsEnum);
    } catch (Exception e) {
      throw new RuntimeException("failed to extract actual tenum", e);
    }
  }

  private static TopDocs _searchTermQuery(IndexSearcher searcher, TermQuery query, int topN, float constantScore) throws IOException {

    List<AtomicReaderContext> leaves = searcher.getIndexReader().leaves();
    //System.out.println("_searchTermQuery: " + leaves.size() + " segments; query=" + query);
    //new Throwable().printStackTrace(System.out);
    Similarity sim = searcher.getSimilarity();

    if (!(sim instanceof DefaultSimilarity)) {
      throw new IllegalArgumentException("searcher.getSimilarity() must be DefaultSimilarity; got: " + sim);
    }
    Term term = query.getTerm();

    String field = term.field();
    String text = term.text();

    Weight w = searcher.createNormalizedWeight(query);

    float[] topScores;
    if (constantScore < 0.0f) {
      topScores = new float[topN+1];
      Arrays.fill(topScores, Float.MIN_VALUE);
    } else {
      topScores = null;
    }

    int[] topDocIDs = new int[topN+1];
    Arrays.fill(topDocIDs, Integer.MAX_VALUE);

    int totalHits = 0;

    float[] normTable = getNormTable();

    for(int readerIDX=0;readerIDX<leaves.size();readerIDX++) {
      AtomicReaderContext ctx = leaves.get(readerIDX);
      SegmentState state = new SegmentState(ctx, field);
      //System.out.println("  seg=" + readerIDX + " base=" + ctx.docBase);
      if (state.normBytes == null) {
        throw new IllegalArgumentException("cannot handle omitNorms field");
      }
      if (state.skip) {
        continue;
      }

      Scorer scorer = w.scorer(ctx, true, false, state.liveDocs);
      if (scorer != null) {

        //System.out.println("    got scorer");
        float termWeight = getTermScorerTermWeight(scorer);

        int singletonDocID;
        long totalTermFreq;

        long address;
        DocsEnum docsEnum = unwrap(getDocsEnum(scorer));
        if (docsEnum.getClass().getName().indexOf("Lucene41PostingsReader") == -1) {
          throw new IllegalArgumentException("must use Lucene41PostingsFormat; got " + docsEnum.getClass().getName());
        }

        int docFreq = getDocFreq(docsEnum);
        long docTermStartFP = getDocTermStartFP(docsEnum);
          
        if (docFreq > 1) {
          IndexInput docIn = getDocIn(docsEnum);
          address = getMMapAddress(docIn);
          singletonDocID = -1;
          totalTermFreq = 0;
        } else {
          // Pulsed
          address = 0;
          singletonDocID = getSingletonDocID(docsEnum);
          totalTermFreq = getTotalTermFreq(docsEnum);;
          assert singletonDocID >= 0;
          assert singletonDocID < state.maxDoc;
        }
        //System.out.println("    singletonDocID=" + singletonDocID + " liveDocs=" + state.liveDocsBytes + " docFreq=" + docFreq + " docsOnly=" + state.docsOnly);
        totalHits += searchSegmentTermQuery(topDocIDs,
                                            topScores,
                                            state.maxDoc,
                                            ctx.docBase,
                                            state.liveDocsBytes,
                                            state.docsOnly,
                                            termWeight,
                                            state.normBytes,
                                            normTable,
                                            singletonDocID,
                                            totalTermFreq,
                                            docFreq,
                                            docTermStartFP,
                                            address);
      }
    }

    return buildTopDocs(topDocIDs, topScores, totalHits, topN, constantScore);
  }

  private static TopDocs _searchPhraseQuery(IndexSearcher searcher, PhraseQuery query, int topN, float constantScore) throws IOException {

    if (query.getSlop() != 0) {
      throw new IllegalArgumentException("can only handle slop=0; got " + query.getSlop());
    }

    List<AtomicReaderContext> leaves = searcher.getIndexReader().leaves();
    //System.out.println("_searchTermQuery: " + leaves.size() + " segments; query=" + query);
    //new Throwable().printStackTrace(System.out);
    Similarity sim = searcher.getSimilarity();

    if (!(sim instanceof DefaultSimilarity)) {
      throw new IllegalArgumentException("searcher.getSimilarity() must be DefaultSimilarity; got: " + sim);
    }

    String field = (String) getFieldObject(query, "org.apache.lucene.search.PhraseQuery", "field");

    Weight w = searcher.createNormalizedWeight(query);

    float[] topScores;
    if (constantScore < 0.0f) {
      topScores = new float[topN+1];
      Arrays.fill(topScores, Float.MIN_VALUE);
    } else {
      topScores = null;
    }

    int[] topDocIDs = new int[topN+1];
    Arrays.fill(topDocIDs, Integer.MAX_VALUE);

    int totalHits = 0;

    float[] normTable = getNormTable();

    for(int readerIDX=0;readerIDX<leaves.size();readerIDX++) {
      AtomicReaderContext ctx = leaves.get(readerIDX);
      SegmentState state = new SegmentState(ctx, field);
      if (state.normBytes == null) {
        throw new IllegalArgumentException("cannot handle omitNorms field");
      }
      if (state.skip) {
        continue;
      }

      FieldInfo fieldInfo = state.reader.getFieldInfos().fieldInfo(field);

      boolean indexHasOffsets = fieldInfo.getIndexOptions().compareTo(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
      
      boolean indexHasPayloads = fieldInfo.hasPayloads();

      Scorer scorer = w.scorer(ctx, true, false, state.liveDocs);
      if (scorer != null) {

        //System.out.println("    got scorer");
        float termWeight = getExactPhraseScorerTermWeight(scorer);

        Object[] chunkStates = (Object[]) getFieldObject(scorer, "org.apache.lucene.search.ExactPhraseScorer", "chunkStates");

        long[] docTermStartFPs = new long[chunkStates.length];
        long[] posTermStartFPs = new long[chunkStates.length];
        int[] singletonDocIDs = new int[chunkStates.length];
        int[] docFreqs = new int[chunkStates.length];
        int[] posOffsets = new int[chunkStates.length];
        long[] totalTermFreqs = new long[chunkStates.length];
        long docFreqAddress = 0;
        long posAddress = 0;

        for(int i=0;i<chunkStates.length;i++) {
          DocsAndPositionsEnum posEnum = (DocsAndPositionsEnum) getFieldObject(chunkStates[i],
                                                                               "org.apache.lucene.search.ExactPhraseScorer$ChunkState",
                                                                               "posEnum");
          posOffsets[i] = getIntField(chunkStates[i],
                                      "org.apache.lucene.search.ExactPhraseScorer$ChunkState",
                                      "offset");
          //System.out.println("posOffset=" + posOffsets[i]);
          if (posEnum.getClass().getName().indexOf("Lucene41PostingsReader") == -1 ||
              posEnum.getClass().getName().indexOf("BlockDocsAndPositionsEnum") == -1) {
            throw new IllegalArgumentException("must use Lucene41PostingsFormat; got " + posEnum.getClass().getName());
          }

          docFreqs[i] = getIntField(posEnum, "org.apache.lucene.codecs.lucene41.Lucene41PostingsReader$BlockDocsAndPositionsEnum", "docFreq");
          totalTermFreqs[i] = getLongField(posEnum, "org.apache.lucene.codecs.lucene41.Lucene41PostingsReader$BlockDocsAndPositionsEnum", "totalTermFreq");
          docTermStartFPs[i] = getLongField(posEnum, "org.apache.lucene.codecs.lucene41.Lucene41PostingsReader$BlockDocsAndPositionsEnum", "docTermStartFP");
          posTermStartFPs[i] = getLongField(posEnum, "org.apache.lucene.codecs.lucene41.Lucene41PostingsReader$BlockDocsAndPositionsEnum", "posTermStartFP");

          if (posAddress == 0) {
            IndexInput posIn = (IndexInput) getFieldObject(posEnum, "org.apache.lucene.codecs.lucene41.Lucene41PostingsReader$BlockDocsAndPositionsEnum", "posIn");
            posAddress = getMMapAddress(unwrap(posIn));
          }
          if (docFreqs[i] > 1) {
            singletonDocIDs[i] = -1;
            if (docFreqAddress == 0) {
              IndexInput docIn = (IndexInput) getFieldObject(posEnum, "org.apache.lucene.codecs.lucene41.Lucene41PostingsReader$BlockDocsAndPositionsEnum", "startDocIn");
              docFreqAddress = getMMapAddress(unwrap(docIn));
            }
          } else {
            // Pulsed
            singletonDocIDs[i] = getIntField(posEnum, "org.apache.lucene.codecs.lucene41.Lucene41PostingsReader$BlockDocsAndPositionsEnum", "singletonDocID");
            assert singletonDocIDs[i] >= 0;
            assert singletonDocIDs[i] < state.maxDoc;
          }
        }

        //System.out.println("  seg=" + state.reader.getSegmentName());
        totalHits += searchSegmentExactPhraseQuery(topDocIDs,
                                                   topScores,
                                                   state.maxDoc,
                                                   ctx.docBase,
                                                   state.liveDocsBytes,
                                                   termWeight,
                                                   state.normBytes,
                                                   normTable,
                                                   singletonDocIDs,
                                                   totalTermFreqs,
                                                   docFreqs,
                                                   docTermStartFPs,
                                                   posTermStartFPs,
                                                   posOffsets,
                                                   docFreqAddress,
                                                   posAddress,
                                                   indexHasPayloads,
                                                   indexHasOffsets);
      }
    }

    return buildTopDocs(topDocIDs, topScores, totalHits, topN, constantScore);
  }

  private static TopDocs _searchBooleanQuery(IndexSearcher searcher, BooleanQuery query, int topN, float constantScore) throws IOException {

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
      return new TopDocs(0, new ScoreDoc[0]);
    }

    String field = null;
    String[] terms = new String[clauses.length];
    final BooleanClause.Occur[] occurs = new BooleanClause.Occur[clauses.length];
    int numMustNotTop = 0;
    for(int i=0;i<clauses.length;i++) {
      BooleanClause clause = clauses[i];
      occurs[i] = clause.getOccur();
      if (occurs[i] == BooleanClause.Occur.MUST_NOT) {
        numMustNotTop++;
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

    int maxCoord = clauses.length-numMustNotTop;
    float[] coordFactors = new float[maxCoord+1];
    for(int i=0;i<coordFactors.length;i++) {
      float f;
      if (coordDisabled) {
        f = 1.0f;
      } else if (maxCoord == 1) {
        f = 1.0f;
      } else {
        f = sim.coord(i, maxCoord);
      }
      coordFactors[i] = f;
    }

    Weight w = searcher.createNormalizedWeight(query);

    List<Weight> subWeights = getBooleanSubWeights(w);

    float[] topScores;
    if (constantScore < 0.0f) {
      topScores = new float[topN+1];
      Arrays.fill(topScores, Float.MIN_VALUE);
    } else {
      topScores = null;
    }

    int[] topDocIDs = new int[topN+1];
    Arrays.fill(topDocIDs, Integer.MAX_VALUE);
    int totalHits = 0;
    float[] normTable = getNormTable();

    for(int readerIDX=0;readerIDX<leaves.size();readerIDX++) {

      AtomicReaderContext ctx = leaves.get(readerIDX);
      SegmentState state = new SegmentState(ctx, field);
      if (state.docsOnly) {
        throw new IllegalArgumentException("cannot handle DOCS_ONLY field");
      }
      if (state.normBytes == null) {
        throw new IllegalArgumentException("cannot handle omitNorms field");
      }
      if (state.skip) {
        continue;
      }

      List<Scorer> scorers = new ArrayList<Scorer>();
      final List<BooleanClause.Occur> occursList = new ArrayList<BooleanClause.Occur>();
      int numMust = 0;
      int numMustNot = 0;
      for(int i=0;i<terms.length;i++) {
        Scorer scorer = subWeights.get(i).scorer(ctx, true, false, state.liveDocs);
        if (scorer != null) {
          scorers.add(scorer);
          occursList.add(occurs[i]);
          if (occurs[i] == BooleanClause.Occur.MUST) {
            numMust++;
          } else if (occurs[i] == BooleanClause.Occur.MUST_NOT) {
            numMustNot++;
          }
        } else if (occurs[i] == BooleanClause.Occur.MUST) {
          scorers.clear();
          break;
        }
      }

      if (numMustNot == terms.length) {
        throw new IllegalArgumentException("at least one clause must not be MUST_NOT");
      }

      if (!scorers.isEmpty()) {
        final float[] termWeights = new float[scorers.size()];
        final int[] singletonDocIDs = new int[scorers.size()];
        final long[] totalTermFreqs = new long[scorers.size()];
        final int[] docFreqs = new int[scorers.size()];
        final long[] docTermStartFPs = new long[scorers.size()];
        long address = 0;

        for(int i=0;i<scorers.size();i++) {
          Scorer scorer = scorers.get(i);
          termWeights[i] = getTermScorerTermWeight(scorer);
          DocsEnum docsEnum = unwrap(getDocsEnum(scorer));
          if (docsEnum.getClass().getName().indexOf("Lucene41PostingsReader") == -1) {
            throw new IllegalArgumentException("must use Lucene41PostingsFormat; got " + docsEnum.getClass().getName());
          }

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
            totalTermFreqs[i] = getTotalTermFreq(docsEnum);
            assert singletonDocIDs[i] >= 0;
          }
        }

        // Sort in order of MUST_NOT, MUST, SHOULD,
        // secondarily by docFreq descending (for MUST_NOT
        // and SHOULD) and docFreq ascending (for MUST):

        // 4.4:
        /*
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
        */

        // 4.3:
        new SorterTemplate() {
          @Override
          protected int compare(int i, int j) {
            BooleanClause.Occur occuri = occursList.get(i);
            BooleanClause.Occur occurj = occursList.get(j);
            if (occuri == occurj) {
              if (occuri == BooleanClause.Occur.MUST) {
                return docFreqs[i] - docFreqs[j];
              } else {
                return docFreqs[j] - docFreqs[i];
              }
            } else if (occuri == BooleanClause.Occur.MUST_NOT || occurj == BooleanClause.Occur.SHOULD) {
              return -1;
            } else {
              return 1;
            }
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

            y = totalTermFreqs[i];
            totalTermFreqs[i] = totalTermFreqs[j];
            totalTermFreqs[j] = y;

            float z = termWeights[i];
            termWeights[i] = termWeights[j];
            termWeights[j] = z;

            BooleanClause.Occur o = occursList.get(i);
            occursList.set(i, occursList.get(j));
            occursList.set(j, o);
          }

          int pivotDocFreq;
          BooleanClause.Occur pivotOccur;

          @Override
          protected void setPivot(int i) {
            pivotDocFreq = docFreqs[i];
            pivotOccur = occursList.get(i);
          }

          @Override
          protected int comparePivot(int i) {
            BooleanClause.Occur occur = occursList.get(i);
            if (pivotOccur == occur) {
              if (pivotOccur == BooleanClause.Occur.MUST) {
                return pivotDocFreq - docFreqs[i];
              } else {
                return docFreqs[i] - pivotDocFreq;
              }
            } else if (pivotOccur == BooleanClause.Occur.MUST_NOT || occur == BooleanClause.Occur.SHOULD) {
              return -1;
            } else {
              return 1;
            }
          }
        }.mergeSort(0, scorers.size()-1);

        /*
        System.out.println("numMustNot=" + numMustNot);
        for(int i=0;i<scorers.size();i++) {
          System.out.println("  docFreqs[" + i + "]=" + docFreqs[i]);
        }
        */
        
        //System.out.println("  seg=" + state.reader.getSegmentName() + " docFreqs=" + Arrays.toString(docFreqs) + " numMustNot=" + numMustNot + " numMust=" + numMust + " docBase=" + ctx.docBase + " coord=" + Arrays.toString(coordFactors));
        totalHits += searchSegmentBooleanQuery(topDocIDs,
                                               topScores,
                                               state.maxDoc,
                                               ctx.docBase,
                                               state.liveDocsBytes,
                                               termWeights,
                                               state.normBytes,
                                               normTable,
                                               coordFactors,
                                               singletonDocIDs,
                                               totalTermFreqs,
                                               docFreqs,
                                               docTermStartFPs,
                                               address,
                                               numMustNot,
                                               numMust);
      }
    }

    return buildTopDocs(topDocIDs, topScores, totalHits, topN, constantScore);
  }

  private static TopDocs buildTopDocs(int[] topDocIDs, float[] topScores, int totalHits, int topN, float constantScore) {
    ScoreDoc[] scoreDocs = new ScoreDoc[Math.min(totalHits, topN)];

    int heapSize = topN;
    //System.out.println("buildTopDocs: totalHits=" + totalHits + " topN=" + topN);

    // Pop off any remaining sentinel values first (only
    // applies when totalHits < topN):
    for(int i=0;i<topN-scoreDocs.length;i++) {
      if (topScores != null) {
        topScores[1] = topScores[heapSize];
      }
      topDocIDs[1] = topDocIDs[heapSize];
      heapSize--;
      downHeap(heapSize, topDocIDs, topScores);
    }

    for(int i=scoreDocs.length-1;i>=0;i--) {
      scoreDocs[i] = new ScoreDoc(topDocIDs[1], topScores == null ? constantScore : topScores[1]);
      if (topScores != null) {
        topScores[1] = topScores[heapSize];
      }
      topDocIDs[1] = topDocIDs[heapSize];
      //System.out.println("  topDocs[" + i + "]=" + scoreDocs[i]);
      heapSize--;
      downHeap(heapSize, topDocIDs, topScores);
    }

    float maxScore;
    if (scoreDocs.length > 0) {
      maxScore = scoreDocs[0].score;
    } else {
      maxScore = Float.NaN;
    }

    //System.out.println("  maxScore=" + maxScore);
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
    if (topScores == null) {
      downHeap(heapSize, topDocIDs);
      return;
    }

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

  private static void downHeap(int heapSize, int[] topDocIDs) {
    int i = 1;
    // save top node
    int savDocID = topDocIDs[i];
    int j = i << 1;            // find smaller child
    int k = j + 1;
    if (k <= heapSize && topDocIDs[k] > topDocIDs[j]) {
      j = k;
    }
    while (j <= heapSize && topDocIDs[j] > savDocID) {
      // shift up child
      topDocIDs[i] = topDocIDs[j];
      i = j;
      j = i << 1;
      k = j + 1;
      if (k <= heapSize && topDocIDs[k] > topDocIDs[j]) {
        j = k;
      }
    }
    // install saved node
    topDocIDs[i] = savDocID;
  }

  // Needed only when running Lucene tests:
  private static IndexInput unwrap(IndexInput in) {
    try {
      String className = in.getClass().getSimpleName();
      if (className.equals("MockIndexInputWrapper") ||
          className.equals("SlowClosingMockIndexInputWrapper") ||
          className.equals("SlowOpeningMockIndexInputWrapper")) {
        final Class<?> x = Class.forName("org.apache.lucene.store.MockIndexInputWrapper");
        Field f = x.getDeclaredField("delegate");
        f.setAccessible(true);
        return (IndexInput) f.get(in);
      } else {
        return in;
      }
    } catch (Exception e) {
      throw new RuntimeException("failed to unwrap IndexInput", e);
    }
  }

  // Needed only when running Lucene tests:
  private static Directory unwrap(Directory dir) {
    try {
      //System.out.println("unwrap: dir=" + dir);
      String className = dir.getClass().getSimpleName();
      if (className.equals("MockDirectoryWrapper") ||
          className.equals("BaseDirectoryWrapper")) {
        Class<?> x = Class.forName("org.apache.lucene.store.BaseDirectoryWrapper");
        Field f = x.getDeclaredField("delegate");
        f.setAccessible(true);
        return (Directory) f.get(dir);
      } else {
        return dir;
      }
    } catch (Exception e) {
      throw new IllegalStateException("reflection failed", e);
    }
  }

  // Needed only when running Lucene tests:
  private static DocsEnum unwrap(DocsEnum docsEnum) {
    try {
      String className = docsEnum.getClass().getSimpleName();
      if (className.equals("AssertingDocsEnum")) {
        Class<?> x = Class.forName("org.apache.lucene.index.FilterAtomicReader$FilterDocsEnum");
        Field f = x.getDeclaredField("in");
        f.setAccessible(true);
        return (DocsEnum) f.get(docsEnum);
      } else {
        return docsEnum;
      }
    } catch (Exception e) {
      throw new IllegalStateException("reflection failed", e);
    }
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
      throw new IllegalStateException("reflection failed", e);
    }
  }

  private static long getMMapAddress(IndexInput in) {
    try {
      Class<?> x = Class.forName("org.apache.lucene.store.NativeMMapDirectory$NativeMMapIndexInput");
      Field f = x.getDeclaredField("address");
      f.setAccessible(true);
      return f.getLong(in);
    } catch (Exception e) {
      throw new IllegalStateException("reflection failed", e);
    }
  }

  private static float getExactPhraseScorerTermWeight(Scorer scorer) {
    try {
      Class<?> x = Class.forName("org.apache.lucene.search.ExactPhraseScorer");
      Field f = x.getDeclaredField("docScorer");
      f.setAccessible(true);
      Object o = f.get(scorer);
      // 4.4:
      //Class<?> y = Class.forName("org.apache.lucene.search.similarities.TFIDFSimilarity$TFIDFSimScorer");
      Class<?> y = Class.forName("org.apache.lucene.search.similarities.TFIDFSimilarity$ExactTFIDFDocScorer");
      Field weightsField = y.getDeclaredField("weightValue");
      weightsField.setAccessible(true);
      return weightsField.getFloat(o);
    } catch (Exception e) {
      throw new IllegalStateException("reflection failed", e);
    }
  }

  private static float getTermScorerTermWeight(Scorer scorer) {
    try {
      Class<?> x = Class.forName("org.apache.lucene.search.TermScorer");
      Field f = x.getDeclaredField("docScorer");
      f.setAccessible(true);
      Object o = f.get(scorer);
      // 4.4:
      //Class<?> y = Class.forName("org.apache.lucene.search.similarities.TFIDFSimilarity$TFIDFSimScorer");
      Class<?> y = Class.forName("org.apache.lucene.search.similarities.TFIDFSimilarity$ExactTFIDFDocScorer");
      Field weightsField = y.getDeclaredField("weightValue");
      weightsField.setAccessible(true);
      return weightsField.getFloat(o);
    } catch (Exception e) {
      throw new IllegalStateException("reflection failed", e);
    }
  }

  private static byte[] getLiveDocsBits(Bits bits) {
    try {
      Class<?> x = Class.forName("org.apache.lucene.codecs.lucene40.BitVector");
      Field f = x.getDeclaredField("bits");
      f.setAccessible(true);
      return (byte[]) f.get(bits);
    } catch (Exception e) {
      throw new IllegalStateException("reflection failed", e);
    }
  }

  private static DocsEnum getDocsEnum(Scorer scorer) {
    try {
      Class<?> x = Class.forName("org.apache.lucene.search.TermScorer");
      Field f = x.getDeclaredField("docsEnum");
      f.setAccessible(true);
      return (DocsEnum) f.get(scorer);
    } catch (Exception e) {
      throw new IllegalStateException("reflection failed", e);
    }
  }

  private static int getDocFreq(DocsEnum docsEnum) {
    try {
      Class<?> x = Class.forName("org.apache.lucene.codecs.lucene41.Lucene41PostingsReader$BlockDocsEnum");
      Field f = x.getDeclaredField("docFreq");
      f.setAccessible(true);
      return f.getInt(docsEnum);
    } catch (Exception e) {
      throw new IllegalStateException("reflection failed", e);
    }
  }
  
  private static long getTotalTermFreq(DocsEnum docsEnum) {
    try {
      return blockDocsEnumTotalTermFreqField.getLong(docsEnum);
    } catch (Exception e) {
      throw new IllegalStateException("reflection failed", e);
    }
  }

  private static IndexInput getDocIn(DocsEnum docsEnum) {
    try {
      final Class<?> x = Class.forName("org.apache.lucene.codecs.lucene41.Lucene41PostingsReader$BlockDocsEnum");
      final Field f = x.getDeclaredField("startDocIn");
      f.setAccessible(true);
      return unwrap((IndexInput) f.get(docsEnum));
    } catch (Exception e) {
      throw new IllegalStateException("reflection failed", e);
    }
  }

  private static long getDocTermStartFP(DocsEnum docsEnum) {
    try {
      final Class<?> x = Class.forName("org.apache.lucene.codecs.lucene41.Lucene41PostingsReader$BlockDocsEnum");
      final Field f = x.getDeclaredField("docTermStartFP");
      f.setAccessible(true);
      return f.getLong(docsEnum);
    } catch (Exception e) {
      throw new IllegalStateException("reflection failed", e);
    }
  }

  private static int getSingletonDocID(DocsEnum docsEnum) {
    try {
      return blockDocsEnumSingletonDocIDField.getInt(docsEnum);
    } catch (Exception e) {
      throw new IllegalStateException("reflection failed", e);
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
      throw new IllegalStateException("reflection failed", e);
    }
  }

  private static byte[] getNormsBytes(NumericDocValues norms) {
    try {
      final Class<?> x = Class.forName("org.apache.lucene.codecs.lucene42.Lucene42DocValuesProducer$3");
      final Field bytesField = x.getDeclaredField("val$bytes");
      bytesField.setAccessible(true);
      return (byte[]) bytesField.get(norms);
    } catch (Exception e) {
      throw new IllegalStateException("reflection failed", e);
    }
  }
}
