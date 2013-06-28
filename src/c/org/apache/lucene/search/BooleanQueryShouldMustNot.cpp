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

static void
orMustNotChunk(PostingsState *sub,
               int endDoc,
               int *docIDs,
               unsigned char *skips) {

  int nextDocID = sub->nextDocID;
  unsigned int *docDeltas = sub->docDeltas;
  unsigned int *freqs = sub->freqs;

  int blockLastRead = sub->docFreqBlockLastRead;
  int blockEnd = sub->docFreqBlockEnd;

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
        nextDocFreqBlock(sub);
        blockLastRead = -1;
        blockEnd = sub->docFreqBlockEnd;
      }
    }
    nextDocID += docDeltas[++blockLastRead];
  }

  sub->nextDocID = nextDocID;
  sub->docFreqBlockLastRead = blockLastRead;
}

static int
orChunkDeletesWithSkip(PostingsState *sub,
                       double *tsCache,
                       float termWeight,
                       int endDoc,
                       unsigned int *filled,
                       int numFilled,
                       int *docIDs,
                       float *scores,
                       unsigned int *coords,
                       unsigned char *liveDocBytes,
                       unsigned char *skips) {

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
        if (docIDs[slot] != nextDocID || skips[slot] == 0) {
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

// Like orChunk, but is "aware" of skip (from previous
// MUST_NOT clauses):

static int
orChunkWithSkip(PostingsState *sub,
                double *tsCache,
                float termWeight,
                int endDoc,
                unsigned int *filled,
                int numFilled,
                int *docIDs,
                float *scores,
                unsigned int *coords,
                unsigned char *skips) {

  //printf("orChunkWithSkip\n");
  int nextDocID = sub->nextDocID;
  unsigned int *docDeltas = sub->docDeltas;
  unsigned int *freqs = sub->freqs;

  int blockLastRead = sub->docFreqBlockLastRead;
  int blockEnd = sub->docFreqBlockEnd;

  if (scores == 0) {
    while (nextDocID < endDoc) {
      //printf("  or: docID=%d\n", nextDocID);
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
      //printf("  or: docID=%d freq=%d\n", nextDocID, freqs[blockLastRead]);fflush(stdout);
      int slot = nextDocID & MASK;
      if (docIDs[slot] != nextDocID || skips[slot] == 0) {
        int freq = freqs[blockLastRead];
        double score;
        if (freq < TERM_SCORES_CACHE_SIZE) {
          score = tsCache[freq];
        } else {
          score = sqrt(freq) * termWeight;
        }
        //printf("    score=%g\n", score);fflush(stdout);

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

  //printf("orChunkWithSkip done\n");

  return numFilled;
}

int booleanQueryShouldMustNot(PostingsState* subs,
                              unsigned char *liveDocsBytes,
                              double **termScoreCache,
                              float *termWeights,
                              int maxDoc,
                              int topN,
                              int numScorers,
                              int docBase,
                              int numMustNot,
                              unsigned int *filled,
                              int *docIDs,
                              float *scores,
                              unsigned int *coords,
                              float *topScores,
                              int *topDocIDs,
                              float *coordFactors,
                              float *normTable,
                              unsigned char *norms,
                              unsigned char *skips,
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
  //printf("smn\n");fflush(stdout);

  while (docUpto < maxDoc) {
    int endDoc = docUpto + CHUNK;
    //printf("cycle endDoc=%d dels=%lx\n", endDoc, liveDocBytes);fflush(stdout);

    int numFilled = 0;

    for(int i=0;i<numMustNot;i++) {
      orMustNotChunk(&subs[i], endDoc, docIDs, skips);
    }
    for(int i=numMustNot;i<numScorers;i++) {
      if (liveDocsBytes != 0) {
        numFilled = orChunkDeletesWithSkip(&subs[i], termScoreCache[i], termWeights[i], endDoc, filled, numFilled, docIDs, scores, coords,
                                           liveDocsBytes, skips);
      } else {
        numFilled = orChunkWithSkip(&subs[i], termScoreCache[i], termWeights[i], endDoc, filled, numFilled, docIDs, scores, coords, skips);
      }
    }

    int docChunkBase = docBase + docUpto;

    if (dsNumDims > 0) {
      hitCount += drillSidewaysCollect(topN,
                                       docBase,
                                       topDocIDs,
                                       topScores,
                                       normTable,
                                       norms,
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
        if (skips[slot]) {
          continue;
        }
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
        if (skips[slot]) {
          continue;
        }
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

    docUpto += CHUNK;
  }

  return hitCount;
}
