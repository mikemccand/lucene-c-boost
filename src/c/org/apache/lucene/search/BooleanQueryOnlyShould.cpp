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

#include <math.h>
#include <stdio.h>

#include "common.h"

static int
orFirstChunk(PostingsState *sub,
             double *tsCache,
             float termWeight,
             int endDoc,
             unsigned int *filled,
             int *docIDs,
             float *scores,
             unsigned int *coords) {

  int nextDocID = sub->nextDocID;
  unsigned int *docDeltas = sub->docDeltas;
  unsigned int *freqs = sub->freqs;

  int blockLastRead = sub->docFreqBlockLastRead;
  int blockEnd = sub->docFreqBlockEnd;

  int numFilled = 0;

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
          nextDocFreqBlock(sub);
          blockLastRead = -1;
          blockEnd = sub->docFreqBlockEnd;
        }
      }
      nextDocID += docDeltas[++blockLastRead];
    }
  } else {

    //printf("scorers[0] nextDoc=%d endDoc=%d\n", nextDocID, endDoc);fflush(stdout);
    while (nextDocID < endDoc) {
      int slot = nextDocID & MASK;
      int freq = freqs[blockLastRead];
      //printf("  docID=%d freq=%d\n", nextDocID, freq);fflush(stdout);
      docIDs[slot] = nextDocID;
      if (freq < TERM_SCORES_CACHE_SIZE) {
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
          nextDocFreqBlock(sub);
          blockLastRead = -1;
          blockEnd = sub->docFreqBlockEnd;
        }
      }
      nextDocID += docDeltas[++blockLastRead];
    }
  }

  sub->nextDocID = nextDocID;
  sub->docFreqBlockLastRead = blockLastRead;

  return numFilled;
}

static int
orFirstChunkDeletes(PostingsState *sub,
                    double *tsCache,
                    float termWeight,
                    int endDoc,
                    unsigned int *filled,
                    int *docIDs,
                    float *scores,
                    unsigned int *coords,
                    unsigned char *liveDocBytes) {

  int nextDocID = sub->nextDocID;
  unsigned int *docDeltas = sub->docDeltas;
  unsigned int *freqs = sub->freqs;

  int blockLastRead = sub->docFreqBlockLastRead;
  int blockEnd = sub->docFreqBlockEnd;

  int numFilled = 0;

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
          nextDocFreqBlock(sub);
          blockLastRead = -1;
          blockEnd = sub->docFreqBlockEnd;
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
        if (freq < TERM_SCORES_CACHE_SIZE) {
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
          nextDocFreqBlock(sub);
          blockLastRead = -1;
          blockEnd = sub->docFreqBlockEnd;
        }
      }
      nextDocID += docDeltas[++blockLastRead];
    }
  }

  sub->nextDocID = nextDocID;
  sub->docFreqBlockLastRead = blockLastRead;

  return numFilled;
}

static int
orChunk(PostingsState *sub,
        double *tsCache,
        float termWeight,
        int endDoc,
        unsigned int *filled,
        int numFilled,
        int *docIDs,
        float *scores,
        unsigned int *coords) {

  int nextDocID = sub->nextDocID;
  unsigned int *docDeltas = sub->docDeltas;
  unsigned int *freqs = sub->freqs;

  int blockLastRead = sub->docFreqBlockLastRead;
  int blockEnd = sub->docFreqBlockEnd;

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
          nextDocFreqBlock(sub);
          blockLastRead = -1;
          blockEnd = sub->docFreqBlockEnd;
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
      if (freq < TERM_SCORES_CACHE_SIZE) {
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
          nextDocFreqBlock(sub);
          blockLastRead = -1;
          blockEnd = sub->docFreqBlockEnd;
        }
      }
      nextDocID += docDeltas[++blockLastRead];
    }
  }

  sub->nextDocID = nextDocID;
  sub->docFreqBlockLastRead = blockLastRead;

  return numFilled;
}

static int
orChunkDeletes(PostingsState *sub,
               double *tsCache,
               float termWeight,
               int endDoc,
               unsigned int *filled,
               int numFilled,
               int *docIDs,
               float *scores,
               unsigned int *coords,
               unsigned char *liveDocBytes) {

  int nextDocID = sub->nextDocID;
  unsigned int *docDeltas = sub->docDeltas;
  unsigned int *freqs = sub->freqs;

  int blockLastRead = sub->docFreqBlockLastRead;
  int blockEnd = sub->docFreqBlockEnd;

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
          nextDocFreqBlock(sub);
          blockLastRead = -1;
          blockEnd = sub->docFreqBlockEnd;
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
        if (freq < TERM_SCORES_CACHE_SIZE) {
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
          nextDocFreqBlock(sub);
          blockLastRead = -1;
          blockEnd = sub->docFreqBlockEnd;
        }
      }
      nextDocID += docDeltas[++blockLastRead];
    }
  }

  sub->nextDocID = nextDocID;
  sub->docFreqBlockLastRead = blockLastRead;

  return numFilled;
}

int booleanQueryOnlyShould(PostingsState* subs,
                           unsigned char *liveDocsBytes,
                           double **termScoreCache,
                           float *termWeights,
                           int maxDoc,
                           int topN,
                           int numScorers,
                           int docBase,
                           unsigned int *filled,
                           int *docIDs,
                           float *scores,
                           unsigned int *coords,
                           float *topScores,
                           int *topDocIDs,
                           float *coordFactors,
                           float *normTable,
                           unsigned char *norms,
                           PostingsState *dsSubs,
                           unsigned int *dsCounts,
                           unsigned int *dsMissingDims,
                           unsigned int dsNumDims,
                           unsigned int *dsTotalHits,
                           unsigned int *dsTermsPerDim,
                           unsigned long *dsHitBits,
                           unsigned long **dsNearMissBits)
{
  int docUpto = 0;
  int hitCount = 0;
  while (docUpto < maxDoc) {
    int endDoc = docUpto + CHUNK;
    printf("cycle endDoc=%d\n", endDoc);fflush(stdout);

    int numFilled = 0;

    if (liveDocsBytes != 0) {
      // Collect first sub without if, since we know every
      // slot will be stale:
      numFilled = orFirstChunkDeletes(&subs[0], termScoreCache[0], termWeights[0], endDoc, filled, docIDs, scores, coords, liveDocsBytes);
      for(int i=1;i<numScorers;i++) {
        numFilled = orChunkDeletes(&subs[i], termScoreCache[i], termWeights[i], endDoc, filled, numFilled, docIDs, scores, coords, liveDocsBytes);
      }
    } else {
      // Collect first sub without if, since we know every
      // slot will be stale:
      numFilled = orFirstChunk(&subs[0], termScoreCache[0], termWeights[0], endDoc, filled, docIDs, scores, coords);
      for(int i=1;i<numScorers;i++) {
        numFilled = orChunk(&subs[i], termScoreCache[i], termWeights[i], endDoc, filled, numFilled, docIDs, scores, coords);
      }
    }

    int docChunkBase = docBase + docUpto;
    printf("chukn dsNumDims=%d\n", dsNumDims);fflush(stdout);

    if (dsNumDims > 0) {
      hitCount += drillSidewaysCollect(topN,
                                       docBase,
                                       topDocIDs,
                                       topScores,
                                       filled,
                                       numFilled,
                                       docIDs,
                                       scores,
                                       dsCounts,
                                       dsMissingDims,
                                       docUpto,
                                       dsSubs,
                                       dsNumDims,
                                       dsTermsPerDim,
                                       dsTotalHits,
                                       dsHitBits,
                                       dsNearMissBits);
    } else if (topScores == 0) {
      hitCount += numFilled;
      for(int i=0;i<numFilled;i++) {
        int slot = filled[i];
        int docID = docChunkBase + slot;
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
      hitCount += numFilled;

      // Collect:
      //printf("collect:\n");
      for(int i=0;i<numFilled;i++) {
        int slot = filled[i];
        float score = scores[slot] * coordFactors[coords[slot]] * normTable[norms[docIDs[slot]]];
        int docID = docChunkBase + slot;
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
    printf("done chukn dsNumDims=%d\n", dsNumDims);fflush(stdout);

    docUpto += CHUNK;
  }

  printf("done\n");

  return hitCount;
}
