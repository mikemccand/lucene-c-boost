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

//
// To see assembly:
//
//   g++ -fpermissive -S -O4 -o test.s -I/usr/local/src/jdk1.6.0_32/include -I/usr/local/src/jdk1.6.0_32/include/linux /l/nativebq/lucene/misc/src/java/org/apache/lucene/search/NativeSearch.cpp
//

#include <sys/types.h>
#include <sys/stat.h>
//#include <unistd.h>
#include <sys/mman.h>
#include <errno.h>
#include <jni.h>
#include <math.h> // for sqrt
#include <stdlib.h> // malloc
#include <string.h> // memcpy

#include "common.h"

//static FILE *fp = fopen("logout.txt", "w");

// Returns 1 if the bit was not previously set
inline static void setLongBit(long *bits, int index) {
  int wordNum = index >> 6;      // div 64
  int bit = index & 0x3f;     // mod 64
  long bitmask = 1L << bit;
  bits[wordNum] |= bitmask;
}

static int getLongBit(long *bits, int index) {
  int wordNum = index >> 6;      // div 64
  int bit = index & 0x3f;     // mod 64
  long bitmask = 1L << bit;
  return (bits[wordNum] & bitmask) != 0;
}


extern "C" JNIEXPORT jint JNICALL
Java_org_apache_lucene_search_NativeSearch_searchSegmentBooleanQuery
  (JNIEnv *env,
   jclass cl,

   // PQ holding top hits so far, pre-filled with sentinel
   // values: 
   jintArray jtopDocIDs,
   jfloatArray jtopScores,

   // Current segment's maxDoc
   jint maxDoc,

   // Current segment's docBase
   jint docBase,

   // Current segment's liveDocs, or null:
   jbyteArray jliveDocsBytes,

   // weightValue from each TermWeight:
   jfloatArray jtermWeights,

   // Norms for the field (all TermQuery must be against a single field):
   jbyteArray jnorms,

   // nocommit silly to pass this once for each segment:
   // Cache, mapping byte norm -> float
   jfloatArray jnormTable,

   // Coord factors from BQ:
   jfloatArray jcoordFactors,

   // If the term has only one docID in this segment (it was
   // "pulsed") then its set here, else -1:
   jintArray jsingletonDocIDs,

   // docFreq of each term
   jintArray jdocFreqs,

   // Offset in the .doc file where this term's docs+freqs begin:
   jlongArray jdocTermStartFPs,

   // Address in memory where .doc file is mapped:
   jlong docFileAddress,

   // First numNot clauses are MUST_NOT
   jint numMustNot,

   // Next numMust clauses are MUST
   jint numMust)
{

  // Clauses come in sorted MUST_NOT (docFreq descending),
  // MUST (docFreq ascending), SHOULD (docFreq descending)

  unsigned long __attribute__ ((aligned(16))) longBuffer[64]; 

  //printf("START search\n"); fflush(stdout);

  float *scores;
  unsigned int *coords;

  if (jtopScores == 0) {
    scores = 0;
    coords = 0;
  } else {
    scores = (float *) malloc(CHUNK * sizeof(float));
    coords = (unsigned int *) malloc(CHUNK * sizeof(int));
  }

  // Set to 1 by a MUST_NOT match:
  unsigned char *skips = (unsigned char *) calloc(CHUNK, sizeof(char));

  int *docIDs = (int *) malloc(CHUNK * sizeof(int));

  for(int i=0;i<CHUNK;i++) {
    docIDs[i] = -1;
  }

  int numScorers = env->GetArrayLength(jdocFreqs);

  int topN = env->GetArrayLength(jtopDocIDs) - 1;
  //printf("topN=%d\n", topN);

  PostingsState *subs = (PostingsState *) malloc(numScorers*sizeof(PostingsState));

  int *singletonDocIDs = env->GetIntArrayElements(jsingletonDocIDs, 0);
  long *docTermStartFPs = env->GetLongArrayElements(jdocTermStartFPs, 0);
  int *docFreqs = env->GetIntArrayElements(jdocFreqs, 0);
  float *termWeights = (float *) env->GetFloatArrayElements(jtermWeights, 0);
  float *coordFactors = (float *) env->GetFloatArrayElements(jcoordFactors, 0);

  unsigned char isCopy = 0;
  unsigned char *liveDocsBytes;
  if (jliveDocsBytes == 0) {
    liveDocsBytes = 0;
  } else {
    liveDocsBytes = (unsigned char *) env->GetPrimitiveArrayCritical(jliveDocsBytes, &isCopy);
    //printf("liveDocs isCopy=%d\n", isCopy);fflush(stdout);
  }

  isCopy = 0;
  unsigned char* norms = (unsigned char *) env->GetPrimitiveArrayCritical(jnorms, &isCopy);
  //printf("norms isCopy=%d\n", isCopy);fflush(stdout);

  isCopy = 0;
  float *normTable = (float *) env->GetPrimitiveArrayCritical(jnormTable, &isCopy);
  //printf("normTable %lx isCopy=%d\n", normTable, isCopy);fflush(stdout);
  unsigned int *freq1 = (unsigned int *) malloc(sizeof(int));
  freq1[0] = 1;
  // Init scorers:
  for(int i=0;i<numScorers;i++) {
    PostingsState *sub = &(subs[i]);
    sub->docsOnly = false;
    sub->id = i;
    //printf("init scorers[%d] of %d\n", i, numScorers);

    if (singletonDocIDs[i] != -1) {
      //printf("  singleton: %d\n", singletonDocIDs[i]);
      sub->nextDocID = singletonDocIDs[i];
      sub->docsLeft = 0;
      sub->blockLastRead = 0;
      sub->blockEnd = 0;
      sub->docDeltas = 0;
      sub->freqs = freq1;
    } else {
      sub->docsLeft = docFreqs[i];
      if (scores != 0 && i >= numMustNot) {
        sub->docDeltas = (unsigned int *) malloc(2*BLOCK_SIZE*sizeof(int));
        // Locality seemed to help here:
        sub->freqs = sub->docDeltas + BLOCK_SIZE;
      } else {
        sub->docDeltas = (unsigned int *) malloc(BLOCK_SIZE*sizeof(int));
        sub->freqs = 0;
      }
      //printf("docFileAddress=%ld startFP=%ld\n", docFileAddress, docTermStartFPs[i]);fflush(stdout);
      sub->p = ((unsigned char *) docFileAddress) + docTermStartFPs[i];
      //printf("  not singleton\n");
      nextBlock(sub);
      sub->nextDocID = sub->docDeltas[0];
      //printf("docDeltas[0]=%d\n", sub->docDeltas[0]);
      sub->blockLastRead = 0;
    }
    //printf("init i=%d nextDocID=%d freq=%d blockEnd=%d singleton=%d\n", i, sub->nextDocID, sub->nextFreq, sub->blockEnd, singletonDocIDs[i]);fflush(stdout);
  }

  // PQ holding top hits:
  int *topDocIDs = (int *) env->GetIntArrayElements(jtopDocIDs, 0);
  float *topScores;
  if (jtopScores != 0) {
    topScores = (float *) env->GetFloatArrayElements(jtopScores, 0);
  } else {
    topScores = 0;
  }

  unsigned int *filled = (unsigned int *) malloc(CHUNK * sizeof(int));

  double **termScoreCache = (double **) malloc(numScorers*sizeof(double*));
  for(int i=0;i<numScorers;i++) {
    termScoreCache[i] = (double *) malloc(TERM_SCORES_CACHE_SIZE*sizeof(double));
    for(int j=0;j<TERM_SCORES_CACHE_SIZE;j++) {
      termScoreCache[i][j] = termWeights[i] * sqrt(j);
    }
  }

  int hitCount;

  if (numMustNot == 0 && numMust == 0) {
    // Only SHOULD
    hitCount = booleanQueryOnlyShould(subs, liveDocsBytes, termScoreCache, termWeights,
                                      maxDoc, topN, numScorers, docBase, filled, docIDs, scores, coords,
                                      topScores, topDocIDs, coordFactors, normTable,
                                      norms);
  } else if (numMust == 0) {
    // At least one MUST_NOT and at least one SHOULD:
    hitCount = booleanQueryShouldMustNot(subs, liveDocsBytes, termScoreCache, termWeights,
                                         maxDoc, topN, numScorers, docBase, numMustNot, filled, docIDs, scores, coords,
                                         topScores, topDocIDs, coordFactors, normTable,
                                         norms, skips);
  } else if (numMustNot == 0) {
    // At least one MUST and zero or more SHOULD:
    hitCount = booleanQueryShouldMust(subs, liveDocsBytes, termScoreCache, termWeights,
                                      maxDoc, topN, numScorers, docBase, numMust, filled, docIDs, scores, coords,
                                      topScores, topDocIDs, coordFactors, normTable,
                                      norms);
  } else {
    // At least one MUST_NOT, at least one MUST and zero or more SHOULD:
    hitCount = booleanQueryShouldMustMustNot(subs, liveDocsBytes, termScoreCache, termWeights,
                                             maxDoc, topN, numScorers, docBase, numMust, numMustNot, filled, docIDs, scores, coords,
                                             topScores, topDocIDs, coordFactors, normTable,
                                             norms);
  }

  env->ReleasePrimitiveArrayCritical(jnorms, norms, JNI_ABORT);
  if (jliveDocsBytes != 0) {
    env->ReleasePrimitiveArrayCritical(jliveDocsBytes, liveDocsBytes, JNI_ABORT);
  }
  env->ReleasePrimitiveArrayCritical(jnormTable, normTable, JNI_ABORT);

  env->ReleaseIntArrayElements(jsingletonDocIDs, singletonDocIDs, JNI_ABORT);
  env->ReleaseLongArrayElements(jdocTermStartFPs, docTermStartFPs, JNI_ABORT);
  env->ReleaseIntArrayElements(jdocFreqs, docFreqs, JNI_ABORT);
  env->ReleaseFloatArrayElements(jtermWeights, termWeights, JNI_ABORT);
  env->ReleaseFloatArrayElements(jcoordFactors, coordFactors, JNI_ABORT);

  env->ReleaseIntArrayElements(jtopDocIDs, topDocIDs, 0);
  if (jtopScores != 0) {
    env->ReleaseFloatArrayElements(jtopScores, topScores, 0);
  }

  for(int i=0;i<numScorers;i++) {
    free(termScoreCache[i]);
  }
  free(termScoreCache);
  free(filled);
  free(docIDs);
  free(skips);
  if (scores != 0) {
    free(scores);
    free(coords);
  }
  free(freq1);
  for(int i=0;i<numScorers;i++) {
    PostingsState *sub = &(subs[i]);
    if (sub->docDeltas != 0) {
      free(sub->docDeltas);
    }
  }

  free(subs);

  return hitCount;
}


extern "C" JNIEXPORT jint JNICALL
Java_org_apache_lucene_search_NativeSearch_searchSegmentTermQuery
  (JNIEnv *env,
   jclass cl,

   // PQ holding top hits so far, pre-filled with sentinel
   // values: 
   jintArray jtopDocIDs,
   jfloatArray jtopScores,

   // Current segment's maxDoc
   jint maxDoc,

   // Current segment's docBase
   jint docBase,

   // Current segment's liveDocs, or null:
   jbyteArray jliveDocBytes,

   // weightValue from each TermWeight:
   jfloat termWeight,

   // Norms for the field (all TermQuery must be against a single field):
   jbyteArray jnorms,

   // nocommit silly to pass this once for each segment:
   // Cache, mapping byte norm -> float
   jfloatArray jnormTable,

   // If the term has only one docID in this segment (it was
   // "pulsed") then its set here, else -1:
   jint singletonDocID,

   // docFreq of each term
   jint docFreq,

   // Offset in the .doc file where this term's docs+freqs begin:
   jlong docTermStartFP,

   // Address in memory where .doc file is mapped:
   jlong docFileAddress)
{
  //printf("START search\n"); fflush(stdout);

  int topN = env->GetArrayLength(jtopDocIDs) - 1;
  //printf("topN=%d\n", topN);

  unsigned char isCopy = 0;
  unsigned char *liveDocBytes;
  if (jliveDocBytes == 0) {
    liveDocBytes = 0;
  } else {
    liveDocBytes = (unsigned char *) env->GetPrimitiveArrayCritical(jliveDocBytes, &isCopy);
    //printf("liveDocs isCopy=%d\n", isCopy);fflush(stdout);
  }

  isCopy = 0;
  unsigned char* norms = (unsigned char *) env->GetPrimitiveArrayCritical(jnorms, &isCopy);
  //printf("norms isCopy=%d\n", isCopy);fflush(stdout);

  isCopy = 0;
  float *normTable = (float *) env->GetPrimitiveArrayCritical(jnormTable, &isCopy);
  //printf("normTable %lx isCopy=%d\n", normTable, isCopy);fflush(stdout);

  register int totalHits = 0;
  // PQ holding top hits:
  int *topDocIDs = (int *) env->GetIntArrayElements(jtopDocIDs, 0);
  float *topScores;
  if (jtopScores == 0) {
    topScores = 0;
  } else {
    topScores = (float *) env->GetFloatArrayElements(jtopScores, 0);
  }

  if (singletonDocID != -1) {
    if (liveDocBytes == 0 || isSet(liveDocBytes, singletonDocID)) {
      topDocIDs[1] = singletonDocID;
      if (jtopScores != 0) {
        topScores[1] = termWeight * sqrt(1) * normTable[norms[singletonDocID]];
      }
      totalHits = 1;
    }
  } else {
    //printf("TQ: blocks\n");fflush(stdout);

#if 0
    // curiously this more straightforward impl is slower:
    PostingsState *sub = (PostingsState *) malloc(sizeof(PostingsState));
    sub->docsOnly = false;
    //printf("  set docsLeft=%d\n", docFreq);
    sub->docsLeft = docFreq;
    // Locality seemed to help here:
    if (jtopScores != 0) {
      sub->docDeltas = (unsigned int *) malloc(2*BLOCK_SIZE*sizeof(int));
      sub->freqs = sub->docDeltas + BLOCK_SIZE;
    } else {
      sub->docDeltas = (unsigned int *) malloc(BLOCK_SIZE*sizeof(int));
      sub->freqs = 0;
    }
    //printf("docFileAddress=%ld startFP=%ld\n", docFileAddress, docTermStartFPs[i]);fflush(stdout);
    sub->p = ((unsigned char *) docFileAddress) + docTermStartFP;
    int hitCount = 0;

    register double *termScoreCache = (double *) malloc(TERM_SCORES_CACHE_SIZE*sizeof(double));
    for(int j=0;j<TERM_SCORES_CACHE_SIZE;j++) {
      termScoreCache[j] = termWeight * sqrt(j);
    }

    register int nextDocID = 0;
    register unsigned int *docDeltas = sub->docDeltas;
    register unsigned int *freqs = sub->freqs;

    register int blockLastRead = sub->blockLastRead;
    register int blockEnd = sub->blockEnd;
  
    if (liveDocBytes != 0) {
      //printf("TQ: has liveDocs\n");fflush(stdout);
      if (topScores == 0) {
        while (sub->docsLeft != 0) {
          nextBlock(sub);
          register int limit = sub->blockEnd+1;
          for(register int i=0;i<limit;i++) {
            nextDocID += docDeltas[i];
            if (isSet(liveDocBytes, nextDocID)) {
              totalHits++;
              int docID = docBase + nextDocID;
              if (docID < topDocIDs[1]) {
                // Hit is competitive   
                topDocIDs[1] = docID;
                downHeapNoScores(topN, topDocIDs);
              }
            }
          }
        }
      } else {
        while (sub->docsLeft != 0) {
          nextBlock(sub);
          register int limit = sub->blockEnd+1;
          //printf("limit=%d\n", limit);
          for(register int i=0;i<limit;i++) {
            nextDocID += docDeltas[i];
            if (isSet(liveDocBytes, nextDocID)) {
              totalHits++;

              float score;
              int freq = freqs[i];

              if (freq < TERM_SCORES_CACHE_SIZE) {
                score = termScoreCache[freq];
              } else {
                score = sqrt(freq) * termWeight;
              }

              score *= normTable[norms[nextDocID]];

              int docID = docBase + nextDocID;

              if (score > topScores[1] || (score == topScores[1] && docID < topDocIDs[1])) {
                // Hit is competitive   
                topDocIDs[1] = docID;
                topScores[1] = score;

                downHeap(topN, topDocIDs, topScores);
                //printf("    **\n");fflush(stdout);
              }
            }
          }
        }
      }
    } else {
      //printf("TQ: no liveDocs\n");fflush(stdout);

      if (topScores == 0) {
        //printf("TQ: has topScores\n");fflush(stdout);
        while (sub->docsLeft != 0) {
          nextBlock(sub);
          register int limit = sub->blockEnd+1;
          for(register int i=0;i<limit;i++) {
            nextDocID += docDeltas[i];

            int docID = docBase + nextDocID;

            // TODO: this is silly: only first topN docs are
            // competitive, after that we should break
            if (docID < topDocIDs[1]) {
              // Hit is competitive   
              topDocIDs[1] = docID;
              downHeapNoScores(topN, topDocIDs);
            }
          }
        }
      } else {
        while (sub->docsLeft != 0) {
          nextBlock(sub);
          register int limit = sub->blockEnd+1;
          for(register int i=0;i<limit;i++) {
            nextDocID += docDeltas[i];

            float score;
            int freq = freqs[i];

            if (freq < TERM_SCORES_CACHE_SIZE) {
              score = termScoreCache[freq];
            } else {
              score = sqrt(freq) * termWeight;
            }

            score *= normTable[norms[nextDocID]];

            int docID = docBase + nextDocID;

            if (score > topScores[1] || (score == topScores[1] && docID < topDocIDs[1])) {
              // Hit is competitive   
              topDocIDs[1] = docID;
              topScores[1] = score;

              downHeap(topN, topDocIDs, topScores);
              //printf("    **\n");fflush(stdout);
            }
          }
        }
      }

      // No deletions, so totalHits == docFreq
      totalHits = docFreq;
    }

#else

    PostingsState *sub = (PostingsState *) malloc(sizeof(PostingsState));
    sub->docsOnly = false;
    sub->docsLeft = docFreq;
    // Locality seemed to help here:
    if (jtopScores != 0) {
      sub->docDeltas = (unsigned int *) malloc(2*BLOCK_SIZE*sizeof(int));
      sub->freqs = sub->docDeltas + BLOCK_SIZE;
    } else {
      sub->docDeltas = (unsigned int *) malloc(BLOCK_SIZE*sizeof(int));
      sub->freqs = 0;
    }
    //printf("docFileAddress=%ld startFP=%ld\n", docFileAddress, docTermStartFPs[i]);fflush(stdout);
    sub->p = ((unsigned char *) docFileAddress) + docTermStartFP;
    //printf("  not singleton\n");
    nextBlock(sub);
    sub->nextDocID = sub->docDeltas[0];
    //printf("docDeltas[0]=%d\n", sub->docDeltas[0]);
    sub->blockLastRead = 0;

    register double *termScoreCache = (double *) malloc(TERM_SCORES_CACHE_SIZE*sizeof(double));
    for(int j=0;j<TERM_SCORES_CACHE_SIZE;j++) {
      termScoreCache[j] = termWeight * sqrt(j);
    }

    register int nextDocID = sub->nextDocID;
    register unsigned int *docDeltas = sub->docDeltas;
    register unsigned int *freqs = sub->freqs;

    register int blockLastRead = sub->blockLastRead;
    register int blockEnd = sub->blockEnd;
  
    if (liveDocBytes != 0) {
      if (topScores == 0) {
        while (true) {

          if (isSet(liveDocBytes, nextDocID)) {
            totalHits++;

            int docID = docBase + nextDocID;

            if (docID < topDocIDs[1]) {
              // Hit is competitive   
              topDocIDs[1] = docID;
              downHeapNoScores(topN, topDocIDs);
            }
          }

          // Inlined nextDoc:
          if (blockLastRead == blockEnd) {
            if (sub->docsLeft == 0) {
              break;
            } else {
              nextBlock(sub);
              blockLastRead = -1;
              blockEnd = sub->blockEnd;
            }
          }

          nextDocID += docDeltas[++blockLastRead];
        }
      } else {
        while (true) {
          if (isSet(liveDocBytes, nextDocID)) {
            totalHits++;

            float score;
            int freq = freqs[blockLastRead];

            if (freq < TERM_SCORES_CACHE_SIZE) {
              score = termScoreCache[freq];
            } else {
              score = sqrt(freq) * termWeight;
            }

            score *= normTable[norms[nextDocID]];

            int docID = docBase + nextDocID;

            if (score > topScores[1] || (score == topScores[1] && docID < topDocIDs[1])) {
              // Hit is competitive   
              topDocIDs[1] = docID;
              topScores[1] = score;

              downHeap(topN, topDocIDs, topScores);

              //printf("    **\n");fflush(stdout);
            }
          }

          // Inlined nextDoc:
          if (blockLastRead == blockEnd) {
            if (sub->docsLeft == 0) {
              break;
            } else {
              nextBlock(sub);
              blockLastRead = -1;
              blockEnd = sub->blockEnd;
            }
          }

          nextDocID += docDeltas[++blockLastRead];
        }
      }
    } else {

      if (topScores == 0) {
        while (true) {

          int docID = docBase + nextDocID;

          // TODO: this is silly: only first topN docs are
          // competitive, after that we should break
          if (docID < topDocIDs[1]) {
            // Hit is competitive   
            topDocIDs[1] = docID;

            downHeapNoScores(topN, topDocIDs);
          }

          // Inlined nextDoc:
          if (blockLastRead == blockEnd) {
            if (sub->docsLeft == 0) {
              break;
            } else {
              nextBlock(sub);
              blockLastRead = -1;
              blockEnd = sub->blockEnd;
            }
          }

          nextDocID += docDeltas[++blockLastRead];
        }
      } else {

        while (true) {

          float score;
          int freq = freqs[blockLastRead];

          if (freq < TERM_SCORES_CACHE_SIZE) {
            score = termScoreCache[freq];
          } else {
            score = sqrt(freq) * termWeight;
          }

          score *= normTable[norms[nextDocID]];

          int docID = docBase + nextDocID;

          if (score > topScores[1] || (score == topScores[1] && docID < topDocIDs[1])) {
            // Hit is competitive   
            topDocIDs[1] = docID;
            topScores[1] = score;

            downHeap(topN, topDocIDs, topScores);
            //printf("    **\n");fflush(stdout);
          }

          // Inlined nextDoc:
          if (blockLastRead == blockEnd) {
            if (sub->docsLeft == 0) {
              break;
            } else {
              nextBlock(sub);
              blockLastRead = -1;
              blockEnd = sub->blockEnd;
            }
          }

          nextDocID += docDeltas[++blockLastRead];
        }
      }

      totalHits = docFreq;
    }
#endif

    free(termScoreCache);
    if (sub->docDeltas != 0) {
      free(sub->docDeltas);
    }

    free(sub);
  }

  env->ReleasePrimitiveArrayCritical(jnorms, norms, JNI_ABORT);
  if (jliveDocBytes != 0) {
    env->ReleasePrimitiveArrayCritical(jliveDocBytes, liveDocBytes, JNI_ABORT);
  }
  env->ReleasePrimitiveArrayCritical(jnormTable, normTable, JNI_ABORT);

  env->ReleaseIntArrayElements(jtopDocIDs, topDocIDs, 0);
  if (jtopScores != 0) {
    env->ReleaseFloatArrayElements(jtopScores, topScores, 0);
  }

  return totalHits;
}

extern "C" JNIEXPORT void JNICALL
Java_org_apache_lucene_search_NativeSearch_fillMultiTermFilter
  (JNIEnv *env,
   jclass cl,
   jlongArray jbits,
   jbyteArray jliveDocsBytes,
   jlong address,
   jlongArray jtermStats,
   jboolean docsOnly) {

  unsigned char isCopy = 0;

  unsigned char *liveDocsBytes;
  if (jliveDocsBytes == 0) {
    liveDocsBytes = 0;
  } else {
    liveDocsBytes = (unsigned char *) env->GetPrimitiveArrayCritical(jliveDocsBytes, &isCopy);
  }

  isCopy = 0;
  long *bits = (long *) env->GetPrimitiveArrayCritical(jbits, &isCopy);

  long *termStats = (long *) env->GetPrimitiveArrayCritical(jtermStats, 0);

  PostingsState *sub = (PostingsState *) malloc(sizeof(PostingsState));
  sub->docsOnly = (bool) docsOnly;
  sub->docDeltas = (unsigned int *) malloc(BLOCK_SIZE * sizeof(int));
  sub->freqs = 0;

  register unsigned int *docDeltas = sub->docDeltas;

  int numTerms = env->GetArrayLength(jtermStats);
  //printf("numTerms=%d\n", numTerms);
  int i = 0;
  while (i < numTerms) {
    sub->docsLeft = (int) termStats[i++];
    sub->p = (unsigned char *) (address + termStats[i++]);
    nextBlock(sub);
    int nextDocID = 0;

    //printf("do term %d docFreq=%d\n", i, sub->docsLeft);fflush(stdout);

    while (true) {
      register int limit = sub->blockEnd+1;
      //printf("  limit %d\n", limit);fflush(stdout);
      if (liveDocsBytes == 0) {
        for(int j=0;j<limit;j++) {
          nextDocID += docDeltas[j];
          setLongBit(bits, nextDocID);
        }
      } else {
        for(int j=0;j<limit;j++) {
          nextDocID += docDeltas[j];
          setLongBit(bits, nextDocID);
        }
      }
      if (sub->docsLeft == 0) {
        break;
      } else {
        nextBlock(sub);
      }
    }
  }

  free(sub->docDeltas);
  free(sub);
  
  if (jliveDocsBytes != 0) {
    env->ReleasePrimitiveArrayCritical(jliveDocsBytes, liveDocsBytes, JNI_ABORT);
  }
  env->ReleasePrimitiveArrayCritical(jtermStats, termStats, JNI_ABORT);
  env->ReleasePrimitiveArrayCritical(jbits, bits, JNI_ABORT);
}
