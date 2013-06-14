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

static int
orFirstChunk(PostingsState *sub,
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
          nextBlock(sub);
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

static int
orFirstChunkDeletes(PostingsState *sub,
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
          nextBlock(sub);
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

static int
orChunk(PostingsState *sub,
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

static int
orChunkDeletes(PostingsState *sub,
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

int booleanQueryOnlyShould(PostingsState* subs,
                           unsigned char *liveDocsBytes,
                           double **termScoreCache,
                           float *termWeights,
                           register int maxDoc,
                           register int topN,
                           register int numScorers,
                           register int docBase,
                           register unsigned int *filled,
                           register int *docIDs,
                           register float *scores,
                           register unsigned int *coords,
                           register float *topScores,
                           register int *topDocIDs,
                           register float *coordFactors,
                           register float *normTable,
                           register unsigned char *norms) {

  int docUpto = 0;
  int hitCount = 0;
  while (docUpto < maxDoc) {
    register int endDoc = docUpto + CHUNK;
    //printf("cycle endDoc=%d dels=%lx\n", endDoc, liveDocBytes);fflush(stdout);

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

    hitCount += numFilled;

    if (topScores == 0) {
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

  return hitCount;
}