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

#include "PostingsState.h"

// exported from decode.cpp:
void nextBlock(unsigned long *longBuffer, PostingsState* sub);

#define NO_MORE_DOCS 2147483647

#define CHUNK 2048
#define MASK (CHUNK-1)

//static FILE *fp = fopen("logout.txt", "w");

bool isSet(unsigned char *bits, unsigned int docID) {
  bool x = (bits[docID >> 3] & (1 << (docID & 7))) != 0;
  //fprintf(fp, "isSet docID=%d ret=%d\n", docID, x);fflush(fp);
  return x;
}

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


static bool
lessThan(int docID1, float score1, int docID2, float score2) {
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

static void
downHeap(int heapSize, int *topDocIDs, float *topScores) {
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

static void
downHeapNoScores(int heapSize, int *topDocIDs) {
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

static int
orFirstChunk(unsigned long *longBuffer,
             PostingsState *sub,
             register double *tsCache,
             register float termWeight,
             register int endDoc,
             register unsigned int *filled,
             register int *docIDs,
             register float *scores,
             register unsigned int *coords) {

  register int nextDocID = sub->nextDocID;
  register unsigned int *docDeltas = sub->docDeltas;
  register unsigned int *freqs = sub->freqs;

  register int blockLastRead = sub->blockLastRead;
  register int blockEnd = sub->blockEnd;

  register int numFilled = 0;

  // First scorer is different because we know slot is
  // "new" for every hit:
  if (scores == 0) {
    while (nextDocID < endDoc) {
      //printf("  docID=%d\n", nextDocID);
      int slot = nextDocID & MASK;
      docIDs[slot] = nextDocID;
      filled[numFilled++] = slot;

      // Inlined nextDoc:
      if (blockLastRead == blockEnd) {
        if (sub->docsLeft == 0) {
          nextDocID = NO_MORE_DOCS;
          break;
        } else {
          nextBlock(longBuffer, sub);
          blockLastRead = -1;
          blockEnd = sub->blockEnd;
        }
      }
      nextDocID += docDeltas[++blockLastRead];
    }
  } else {

    //printf("scorers[0]\n");fflush(stdout);
    while (nextDocID < endDoc) {
      //printf("  docID=%d\n", nextDocID);
      int slot = nextDocID & MASK;
      int freq = freqs[blockLastRead];
      docIDs[slot] = nextDocID;
      if (freq < 32) {
        scores[slot] = tsCache[freq];
      } else {
        scores[slot] = sqrt(freq) * termWeight;
      }
      coords[slot] = 1;
      filled[numFilled++] = slot;

      // Inlined nextDoc:
      if (blockLastRead == blockEnd) {
        if (sub->docsLeft == 0) {
          nextDocID = NO_MORE_DOCS;
          break;
        } else {
          nextBlock(longBuffer, sub);
          blockLastRead = -1;
          blockEnd = sub->blockEnd;
        }
      }
      nextDocID += docDeltas[++blockLastRead];
    }
  }

  sub->nextDocID = nextDocID;
  sub->blockLastRead = blockLastRead;

  return numFilled;
}

static int
orFirstChunkDeletes(unsigned long *longBuffer,
                    PostingsState *sub,
                    register double *tsCache,
                    register float termWeight,
                    register int endDoc,
                    register unsigned int *filled,
                    register int *docIDs,
                    register float *scores,
                    register unsigned int *coords,
                    register unsigned char *liveDocBytes) {

  register int nextDocID = sub->nextDocID;
  register unsigned int *docDeltas = sub->docDeltas;
  register unsigned int *freqs = sub->freqs;

  register int blockLastRead = sub->blockLastRead;
  register int blockEnd = sub->blockEnd;

  register int numFilled = 0;

  // First scorer is different because we know slot is
  // "new" for every hit:
  if (scores == 0) {
    //printf("scorers[0]\n");
    while (nextDocID < endDoc) {
      if (isSet(liveDocBytes, nextDocID)) {
        //printf("  docID=%d\n", nextDocID);
        int slot = nextDocID & MASK;
        docIDs[slot] = nextDocID;
        filled[numFilled++] = slot;
      }

      // Inlined nextDoc:
      if (blockLastRead == blockEnd) {
        if (sub->docsLeft == 0) {
          nextDocID = NO_MORE_DOCS;
          break;
        } else {
          nextBlock(longBuffer, sub);
          blockLastRead = -1;
          blockEnd = sub->blockEnd;
        }
      }
      nextDocID += docDeltas[++blockLastRead];
    }
  } else {

    // First scorer is different because we know slot is
    // "new" for every hit:
    //printf("scorers[0]\n");
    while (nextDocID < endDoc) {
      if (isSet(liveDocBytes, nextDocID)) {
        int slot = nextDocID & MASK;
        int freq = freqs[blockLastRead];
        docIDs[slot] = nextDocID;
        if (freq < 32) {
          scores[slot] = tsCache[freq];
        } else {
          scores[slot] = sqrt(freq) * termWeight;
        }
        coords[slot] = 1;
        filled[numFilled++] = slot;
      }

      // Inlined nextDoc:
      if (blockLastRead == blockEnd) {
        if (sub->docsLeft == 0) {
          nextDocID = NO_MORE_DOCS;
          break;
        } else {
          nextBlock(longBuffer, sub);
          blockLastRead = -1;
          blockEnd = sub->blockEnd;
        }
      }
      nextDocID += docDeltas[++blockLastRead];
    }
  }

  sub->nextDocID = nextDocID;
  sub->blockLastRead = blockLastRead;

  return numFilled;
}

static int
orChunk(unsigned long *longBuffer,
        PostingsState *sub,
        register double *tsCache,
        register float termWeight,
        register int endDoc,
        register unsigned int *filled,
        int numFilled,
        register int *docIDs,
        register float *scores,
        register unsigned int *coords) {

  register int nextDocID = sub->nextDocID;
  register unsigned int *docDeltas = sub->docDeltas;
  register unsigned int *freqs = sub->freqs;

  register int blockLastRead = sub->blockLastRead;
  register int blockEnd = sub->blockEnd;

  if (scores == 0) {
    while (nextDocID < endDoc) {
      //printf("  docID=%d\n", nextDocID);
      int slot = nextDocID & MASK;

      if (docIDs[slot] != nextDocID) {
        docIDs[slot] = nextDocID;
        filled[numFilled++] = slot;
      }

      // Inlined nextDoc:
      if (blockLastRead == blockEnd) {
        if (sub->docsLeft == 0) {
          nextDocID = NO_MORE_DOCS;
          break;
        } else {
          nextBlock(longBuffer, sub);
          blockLastRead = -1;
          blockEnd = sub->blockEnd;
        }
      }
      nextDocID += docDeltas[++blockLastRead];
    }
  } else {

    //printf("term=%d nextDoc=%d\n", i, sub->nextDocID);
    while (nextDocID < endDoc) {
      //printf("  docID=%d\n", nextDocID);
      int slot = nextDocID & MASK;
      int freq = freqs[blockLastRead];
      double score;
      if (freq < 32) {
        score = tsCache[freq];
      } else {
        score = sqrt(freq) * termWeight;
      }

      if (docIDs[slot] != nextDocID) {
        docIDs[slot] = nextDocID;
        scores[slot] = score;
        coords[slot] = 1;
        filled[numFilled++] = slot;
      } else {
        scores[slot] += score;
        coords[slot]++;
      }

      // Inlined nextDoc:
      if (blockLastRead == blockEnd) {
        if (sub->docsLeft == 0) {
          nextDocID = NO_MORE_DOCS;
          break;
        } else {
          nextBlock(longBuffer, sub);
          blockLastRead = -1;
          blockEnd = sub->blockEnd;
        }
      }
      nextDocID += docDeltas[++blockLastRead];
    }
  }

  sub->nextDocID = nextDocID;
  sub->blockLastRead = blockLastRead;

  return numFilled;
}


// Like orChunk, but is "aware" of skip (from previous
// MUST_NOT clauses):

static int
orChunkWithSkip(unsigned long *longBuffer,
                PostingsState *sub,
                register double *tsCache,
                register float termWeight,
                register int endDoc,
                register unsigned int *filled,
                int numFilled,
                register int *docIDs,
                register float *scores,
                register unsigned int *coords,
                register unsigned char *skips) {

  //printf("orChunkWithSkip\n");
  register int nextDocID = sub->nextDocID;
  register unsigned int *docDeltas = sub->docDeltas;
  register unsigned int *freqs = sub->freqs;

  register int blockLastRead = sub->blockLastRead;
  register int blockEnd = sub->blockEnd;

  if (scores == 0) {
    while (nextDocID < endDoc) {
      //printf("  docID=%d\n", nextDocID);
      int slot = nextDocID & MASK;

      if (docIDs[slot] != nextDocID) {
        docIDs[slot] = nextDocID;
        skips[slot] = 0;
        filled[numFilled++] = slot;
      }

      // Inlined nextDoc:
      if (blockLastRead == blockEnd) {
        if (sub->docsLeft == 0) {
          nextDocID = NO_MORE_DOCS;
          break;
        } else {
          nextBlock(longBuffer, sub);
          blockLastRead = -1;
          blockEnd = sub->blockEnd;
        }
      }
      nextDocID += docDeltas[++blockLastRead];
    }
  } else {

    //printf("term=%d nextDoc=%d\n", i, sub->nextDocID);
    while (nextDocID < endDoc) {
      //printf("  docID=%d\n", nextDocID);
      int slot = nextDocID & MASK;
      if (docIDs[slot] != nextDocID || skips[slot] == 0) {
        int freq = freqs[blockLastRead];
        double score;
        if (freq < 32) {
          score = tsCache[freq];
        } else {
          score = sqrt(freq) * termWeight;
        }

        if (docIDs[slot] != nextDocID) {
          docIDs[slot] = nextDocID;
          scores[slot] = score;
          coords[slot] = 1;
          skips[slot] = 0;
          filled[numFilled++] = slot;
        } else {
          scores[slot] += score;
          coords[slot]++;
        }
      }

      // Inlined nextDoc:
      if (blockLastRead == blockEnd) {
        if (sub->docsLeft == 0) {
          nextDocID = NO_MORE_DOCS;
          break;
        } else {
          nextBlock(longBuffer, sub);
          blockLastRead = -1;
          blockEnd = sub->blockEnd;
        }
      }
      nextDocID += docDeltas[++blockLastRead];
    }
  }

  sub->nextDocID = nextDocID;
  sub->blockLastRead = blockLastRead;

  //printf("orChunkWithSkip done\n");

  return numFilled;
}

static void
orMustNotChunk(unsigned long *longBuffer,
               PostingsState *sub,
               register int endDoc,
               register int *docIDs,
               register unsigned char *skips) {

  register int nextDocID = sub->nextDocID;
  register unsigned int *docDeltas = sub->docDeltas;
  register unsigned int *freqs = sub->freqs;

  register int blockLastRead = sub->blockLastRead;
  register int blockEnd = sub->blockEnd;

  while (nextDocID < endDoc) {
    //printf("  docID=%d\n", nextDocID);
    int slot = nextDocID & MASK;
    docIDs[slot] = nextDocID;
    skips[slot] = 1;

    // Inlined nextDoc:
    if (blockLastRead == blockEnd) {
      if (sub->docsLeft == 0) {
        nextDocID = NO_MORE_DOCS;
        break;
      } else {
        nextBlock(longBuffer, sub);
        blockLastRead = -1;
        blockEnd = sub->blockEnd;
      }
    }
    nextDocID += docDeltas[++blockLastRead];
  }

  sub->nextDocID = nextDocID;
  sub->blockLastRead = blockLastRead;
}

static int
orChunkDeletes(unsigned long *longBuffer,
               PostingsState *sub,
               register double *tsCache,
               register float termWeight,
               register int endDoc,
               register unsigned int *filled,
               int numFilled,
               register int *docIDs,
               register float *scores,
               register unsigned int *coords,
               register unsigned char *liveDocBytes) {

  register int nextDocID = sub->nextDocID;
  register unsigned int *docDeltas = sub->docDeltas;
  register unsigned int *freqs = sub->freqs;

  register int blockLastRead = sub->blockLastRead;
  register int blockEnd = sub->blockEnd;

  if (scores == 0) {
    //printf("term=%d nextDoc=%d\n", i, sub->nextDocID);
    while (nextDocID < endDoc) {
      //printf("  docID=%d\n", nextDocID);
      if (isSet(liveDocBytes, nextDocID)) {
        int slot = nextDocID & MASK;

        if (docIDs[slot] != nextDocID) {
          docIDs[slot] = nextDocID;
          filled[numFilled++] = slot;
        }
      }

      // Inlined nextDoc:
      if (blockLastRead == blockEnd) {
        if (sub->docsLeft == 0) {
          nextDocID = NO_MORE_DOCS;
          break;
        } else {
          nextBlock(longBuffer, sub);
          blockLastRead = -1;
          blockEnd = sub->blockEnd;
        }
      }
      nextDocID += docDeltas[++blockLastRead];
    }
  } else {

    //printf("term=%d nextDoc=%d\n", i, sub->nextDocID);
    while (nextDocID < endDoc) {
      //printf("  docID=%d\n", nextDocID);
      if (isSet(liveDocBytes, nextDocID)) {
        int slot = nextDocID & MASK;
        int freq = freqs[blockLastRead];
        double score;
        if (freq < 32) {
          score = tsCache[freq];
        } else {
          score = sqrt(freq) * termWeight;
        }

        if (docIDs[slot] != nextDocID) {
          docIDs[slot] = nextDocID;
          scores[slot] = score;
          coords[slot] = 1;
          filled[numFilled++] = slot;
        } else {
          scores[slot] += score;
          coords[slot]++;
        }
      }

      // Inlined nextDoc:
      if (blockLastRead == blockEnd) {
        if (sub->docsLeft == 0) {
          nextDocID = NO_MORE_DOCS;
          break;
        } else {
          nextBlock(longBuffer, sub);
          blockLastRead = -1;
          blockEnd = sub->blockEnd;
        }
      }
      nextDocID += docDeltas[++blockLastRead];
    }
  }

  sub->nextDocID = nextDocID;
  sub->blockLastRead = blockLastRead;

  return numFilled;
}

static int
orChunkDeletesWithSkip(unsigned long *longBuffer,
                       PostingsState *sub,
                       register double *tsCache,
                       register float termWeight,
                       register int endDoc,
                       register unsigned int *filled,
                       int numFilled,
                       register int *docIDs,
                       register float *scores,
                       register unsigned int *coords,
                       register unsigned char *liveDocBytes,
                       register unsigned char *skips) {

  register int nextDocID = sub->nextDocID;
  register unsigned int *docDeltas = sub->docDeltas;
  register unsigned int *freqs = sub->freqs;

  register int blockLastRead = sub->blockLastRead;
  register int blockEnd = sub->blockEnd;

  if (scores == 0) {
    //printf("term=%d nextDoc=%d\n", i, sub->nextDocID);
    while (nextDocID < endDoc) {
      //printf("  docID=%d\n", nextDocID);
      if (isSet(liveDocBytes, nextDocID)) {
        int slot = nextDocID & MASK;

        if (docIDs[slot] != nextDocID) {
          docIDs[slot] = nextDocID;
          skips[slot] = 0;
          filled[numFilled++] = slot;
        }
      }

      // Inlined nextDoc:
      if (blockLastRead == blockEnd) {
        if (sub->docsLeft == 0) {
          nextDocID = NO_MORE_DOCS;
          break;
        } else {
          nextBlock(longBuffer, sub);
          blockLastRead = -1;
          blockEnd = sub->blockEnd;
        }
      }
      nextDocID += docDeltas[++blockLastRead];
    }
  } else {

    //printf("term=%d nextDoc=%d\n", i, sub->nextDocID);
    while (nextDocID < endDoc) {
      //printf("  docID=%d\n", nextDocID);
      if (isSet(liveDocBytes, nextDocID)) {
        int slot = nextDocID & MASK;
        if (docIDs[slot] != nextDocID || skips[slot] == 0) {
          int freq = freqs[blockLastRead];
          double score;
          if (freq < 32) {
            score = tsCache[freq];
          } else {
            score = sqrt(freq) * termWeight;
          }

          if (docIDs[slot] != nextDocID) {
            docIDs[slot] = nextDocID;
            scores[slot] = score;
            coords[slot] = 1;
            skips[slot] = 0;
            filled[numFilled++] = slot;
          } else {
            scores[slot] += score;
            coords[slot]++;
          }
        }
      }

      // Inlined nextDoc:
      if (blockLastRead == blockEnd) {
        if (sub->docsLeft == 0) {
          nextDocID = NO_MORE_DOCS;
          break;
        } else {
          nextBlock(longBuffer, sub);
          blockLastRead = -1;
          blockEnd = sub->blockEnd;
        }
      }
      nextDocID += docDeltas[++blockLastRead];
    }
  }

  sub->nextDocID = nextDocID;
  sub->blockLastRead = blockLastRead;

  return numFilled;
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
   jbyteArray jliveDocBytes,

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
      nextBlock(longBuffer, sub);
      sub->nextDocID = sub->docDeltas[0];
      //printf("docDeltas[0]=%d\n", sub->docDeltas[0]);
      sub->blockLastRead = 0;
    }
    //printf("init i=%d nextDocID=%d freq=%d blockEnd=%d singleton=%d\n", i, sub->nextDocID, sub->nextFreq, sub->blockEnd, singletonDocIDs[i]);fflush(stdout);
  }

  int docUpto = 0;

  // PQ holding top hits:
  int *topDocIDs = (int *) env->GetIntArrayElements(jtopDocIDs, 0);
  float *topScores;
  if (jtopScores != 0) {
    topScores = (float *) env->GetFloatArrayElements(jtopScores, 0);
  } else {
    topScores = 0;
  }
  unsigned int *filled = (unsigned int *) malloc(CHUNK * sizeof(int));
  int hitCount = 0;

  double **termScoreCache = (double **) malloc(numScorers*sizeof(double*));
  for(int i=0;i<numScorers;i++) {
    termScoreCache[i] = (double *) malloc(32*sizeof(double));
    for(int j=0;j<32;j++) {
      termScoreCache[i][j] = termWeights[i] * sqrt(j);
    }
  }

  while (docUpto < maxDoc) {
    register int endDoc = docUpto + CHUNK;
    //printf("cycle endDoc=%d dels=%lx\n", endDoc, liveDocBytes);fflush(stdout);

    int numFilled = 0;

    if (numMustNot != 0) {
      for(int i=0;i<numMustNot;i++) {
        orMustNotChunk(longBuffer, &subs[i], endDoc, docIDs, skips);
      }
      for(int i=numMustNot;i<numScorers;i++) {
        if (liveDocBytes != 0) {
          numFilled = orChunkDeletesWithSkip(longBuffer, &subs[i], termScoreCache[i], termWeights[i], endDoc, filled, numFilled, docIDs, scores, coords,
                                             liveDocBytes, skips);
        } else {
          numFilled = orChunkWithSkip(longBuffer, &subs[i], termScoreCache[i], termWeights[i], endDoc, filled, numFilled, docIDs, scores, coords, skips);
        }
      }
    } else if (numMust != 0) {
      // nocommit todo
    } else if (liveDocBytes != 0) {
      // Collect first sub without if, since we know every
      // slot will be stale:
      numFilled = orFirstChunkDeletes(longBuffer, &subs[0], termScoreCache[0], termWeights[0], endDoc, filled, docIDs, scores, coords, liveDocBytes);
      for(int i=1;i<numScorers;i++) {
        numFilled = orChunkDeletes(longBuffer, &subs[i], termScoreCache[i], termWeights[i], endDoc, filled, numFilled, docIDs, scores, coords, liveDocBytes);
      }
    } else {
      // Collect first sub without if, since we know every
      // slot will be stale:
      numFilled = orFirstChunk(longBuffer, &subs[0], termScoreCache[0], termWeights[0], endDoc, filled, docIDs, scores, coords);
      for(int i=1;i<numScorers;i++) {
        numFilled = orChunk(longBuffer, &subs[i], termScoreCache[i], termWeights[i], endDoc, filled, numFilled, docIDs, scores, coords);
      }
    }

    hitCount += numFilled;

    if (numMustNot != 0) {
      if (topScores == 0) {
        for(int i=0;i<numFilled;i++) {
          int slot = filled[i];
          if (skips[slot]) {
            continue;
          }
          int docID = docBase + docIDs[slot];
          // TODO: we can stop collecting, and tracking filled,
          // after chunk once queue is full
          if (docID < topDocIDs[1]) {
            // Hit is competitive   
            topDocIDs[1] = docID;
            downHeapNoScores(topN, topDocIDs);
            //printf("    **\n");fflush(stdout);
          }
        }
      } else {

        // Collect:
        //printf("collect:\n");
        for(int i=0;i<numFilled;i++) {
          int slot = filled[i];
          if (skips[slot]) {
            continue;
          }
          float score = scores[slot] * coordFactors[coords[slot]] * normTable[norms[docIDs[slot]]];
          int docID = docBase + docIDs[slot];
          //printf("  docBase=%d doc=%d score=%.5f coord=%d cf=%.5f\n",
          //docBase, docID, score, coords[slot], coordFactors[coords[slot]]);

          if (score > topScores[1] || (score == topScores[1] && docID < topDocIDs[1])) {
            // Hit is competitive   
            topDocIDs[1] = docID;
            topScores[1] = score;

            downHeap(topN, topDocIDs, topScores);
        
            //printf("    **\n");fflush(stdout);
          }
        }
      }
    } else if (topScores == 0) {
      for(int i=0;i<numFilled;i++) {
        int slot = filled[i];
        int docID = docBase + docIDs[slot];
        // TODO: we can stop collecting, and tracking filled,
        // after chunk once queue is full
        if (docID < topDocIDs[1]) {
          // Hit is competitive   
          topDocIDs[1] = docID;
          downHeapNoScores(topN, topDocIDs);
          //printf("    **\n");fflush(stdout);
        }
      }
    } else {

      // Collect:
      //printf("collect:\n");
      for(int i=0;i<numFilled;i++) {
        int slot = filled[i];
        float score = scores[slot] * coordFactors[coords[slot]] * normTable[norms[docIDs[slot]]];
        int docID = docBase + docIDs[slot];
        //printf("  docBase=%d doc=%d score=%.5f coord=%d cf=%.5f\n",
        //docBase, docID, score, coords[slot], coordFactors[coords[slot]]);

        if (score > topScores[1] || (score == topScores[1] && docID < topDocIDs[1])) {
          // Hit is competitive   
          topDocIDs[1] = docID;
          topScores[1] = score;

          downHeap(topN, topDocIDs, topScores);
        
          //printf("    **\n");fflush(stdout);
        }
      }
    }

    docUpto += CHUNK;
  }

  env->ReleasePrimitiveArrayCritical(jnorms, norms, JNI_ABORT);
  if (jliveDocBytes != 0) {
    env->ReleasePrimitiveArrayCritical(jliveDocBytes, liveDocBytes, JNI_ABORT);
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
  unsigned long __attribute__ ((aligned(16))) longBuffer[64]; 

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

    register double *termScoreCache = (double *) malloc(32*sizeof(double));
    for(int j=0;j<32;j++) {
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
          nextBlock(longBuffer, sub);
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
          nextBlock(longBuffer, sub);
          register int limit = sub->blockEnd+1;
          //printf("limit=%d\n", limit);
          for(register int i=0;i<limit;i++) {
            nextDocID += docDeltas[i];
            if (isSet(liveDocBytes, nextDocID)) {
              totalHits++;

              float score;
              int freq = freqs[i];

              if (freq < 32) {
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
          nextBlock(longBuffer, sub);
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
          nextBlock(longBuffer, sub);
          register int limit = sub->blockEnd+1;
          for(register int i=0;i<limit;i++) {
            nextDocID += docDeltas[i];

            float score;
            int freq = freqs[i];

            if (freq < 32) {
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
    nextBlock(longBuffer, sub);
    sub->nextDocID = sub->docDeltas[0];
    //printf("docDeltas[0]=%d\n", sub->docDeltas[0]);
    sub->blockLastRead = 0;

    register double *termScoreCache = (double *) malloc(32*sizeof(double));
    for(int j=0;j<32;j++) {
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
              nextBlock(longBuffer, sub);
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

            if (freq < 32) {
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
              nextBlock(longBuffer, sub);
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
              nextBlock(longBuffer, sub);
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

          if (freq < 32) {
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
              nextBlock(longBuffer, sub);
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

  unsigned long __attribute__ ((aligned(16))) longBuffer[64]; 
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
    nextBlock(longBuffer, sub);
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
        nextBlock(longBuffer, sub);
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

