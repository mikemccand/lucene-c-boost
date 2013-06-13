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

#include "common.h"

static void
orMustNotChunk(PostingsState *sub,
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
        nextBlock(sub);
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
orChunkDeletesWithSkip(PostingsState *sub,
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
          nextBlock(sub);
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
          nextBlock(sub);
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
orChunkWithSkip(PostingsState *sub,
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
          nextBlock(sub);
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

      // Inlined nextDoc:
      if (blockLastRead == blockEnd) {
        if (sub->docsLeft == 0) {
          nextDocID = NO_MORE_DOCS;
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

  sub->nextDocID = nextDocID;
  sub->blockLastRead = blockLastRead;

  //printf("orChunkWithSkip done\n");

  return numFilled;
}

int booleanQueryShouldMustNot(PostingsState* subs,
                              unsigned char *liveDocsBytes,
                              double **termScoreCache,
                              float *termWeights,
                              register int maxDoc,
                              register int topN,
                              register int numScorers,
                              register int docBase,
                              register int numMustNot,
                              register unsigned int *filled,
                              register int *docIDs,
                              register float *scores,
                              register unsigned int *coords,
                              register float *topScores,
                              register int *topDocIDs,
                              register float *coordFactors,
                              register float *normTable,
                              register unsigned char *norms,
                              register unsigned char *skips) {

  int docUpto = 0;
  int hitCount = 0;

  while (docUpto < maxDoc) {
    register int endDoc = docUpto + CHUNK;
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

    hitCount += numFilled;

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

    docUpto += CHUNK;
  }

  return hitCount;
}
