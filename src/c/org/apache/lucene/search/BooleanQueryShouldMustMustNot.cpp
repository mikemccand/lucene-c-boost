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
//#include <stdio.h>

#include "common.h"

static void
orMustNotChunk(PostingsState *sub,
               register int endDoc,
               register int *docIDs,
               register unsigned int *coords) {

  register int nextDocID = sub->nextDocID;
  register unsigned int *docDeltas = sub->docDeltas;
  register unsigned int *freqs = sub->freqs;

  register int blockLastRead = sub->docFreqBlockLastRead;
  register int blockEnd = sub->docFreqBlockEnd;

  while (nextDocID < endDoc) {
    //printf("  docID=%d\n", nextDocID);
    int slot = nextDocID & MASK;
    docIDs[slot] = nextDocID;
    coords[slot] = 0;

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
orFirstMustChunk(PostingsState *sub,
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

  register int blockLastRead = sub->docFreqBlockLastRead;
  register int blockEnd = sub->docFreqBlockEnd;

  register int numFilled = 0;

  // First scorer is different because we know slot is
  // "new" for every hit:
  if (scores == 0) {
    while (nextDocID < endDoc) {
      //printf("  docID=%d\n", nextDocID);
      int slot = nextDocID & MASK;
      if (docIDs[slot] != nextDocID) {
        docIDs[slot] = nextDocID;
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
  } else {

    //printf("scorers[0]\n");fflush(stdout);
    while (nextDocID < endDoc) {
      //printf("  docID=%d\n", nextDocID);
      int slot = nextDocID & MASK;
      if (docIDs[slot] != nextDocID) {
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
  //printf("return numFilled=%d\n", numFilled);
  return numFilled;
}

static int
orFirstMustChunkWithDeletes(PostingsState *sub,
                            register double *tsCache,
                            register float termWeight,
                            register int endDoc,
                            register unsigned int *filled,
                            register int *docIDs,
                            register float *scores,
                            register unsigned int *coords,
                            register unsigned char *liveDocsBytes) {

  register int nextDocID = sub->nextDocID;
  register unsigned int *docDeltas = sub->docDeltas;
  register unsigned int *freqs = sub->freqs;

  register int blockLastRead = sub->docFreqBlockLastRead;
  register int blockEnd = sub->docFreqBlockEnd;

  register int numFilled = 0;

  // First scorer is different because we know slot is
  // "new" for every hit:
  if (scores == 0) {
    while (nextDocID < endDoc) {
      if (isSet(liveDocsBytes, nextDocID)) {
        //printf("  docID=%d\n", nextDocID);
        int slot = nextDocID & MASK;
        if (docIDs[slot] != nextDocID) {
          docIDs[slot] = nextDocID;
          coords[slot] = 1;
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

    //printf("scorers[0]\n");fflush(stdout);
    while (nextDocID < endDoc) {
      //printf("  docID=%d\n", nextDocID);
      if (isSet(liveDocsBytes, nextDocID)) {
        int slot = nextDocID & MASK;
        if (docIDs[slot] != nextDocID) {
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
  //printf("return numFilled=%d\n", numFilled);
  return numFilled;
}


static void
orShouldChunk(PostingsState *sub,
              register double *tsCache,
              register float termWeight,
              register int endDoc,
              register int *docIDs,
              register float *scores,
              register unsigned int *coords,
              register int prevMustClauseCount) {
              
  register int nextDocID = sub->nextDocID;
  register unsigned int *docDeltas = sub->docDeltas;
  register unsigned int *freqs = sub->freqs;

  register int blockLastRead = sub->docFreqBlockLastRead;
  register int blockEnd = sub->docFreqBlockEnd;

  //printf("term=%d nextDoc=%d\n", i, sub->nextDocID);
  while (nextDocID < endDoc) {
    //printf("  docID=%d\n", nextDocID);
    int slot = nextDocID & MASK;
    if (docIDs[slot] == nextDocID && coords[slot] >= prevMustClauseCount) {

      int freq = freqs[blockLastRead];
      double score;
      if (freq < TERM_SCORES_CACHE_SIZE) {
        score = tsCache[freq];
      } else {
        score = sqrt(freq) * termWeight;
      }

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

  sub->nextDocID = nextDocID;
  sub->docFreqBlockLastRead = blockLastRead;
}

static void
orMustChunk(PostingsState *sub,
            register double *tsCache,
            register float termWeight,
            register int endDoc,
            register int *docIDs,
            register float *scores,
            register unsigned int *coords,
            register int prevMustClauseCount) {

  register int nextDocID = sub->nextDocID;
  register unsigned int *docDeltas = sub->docDeltas;
  register unsigned int *freqs = sub->freqs;

  register int blockLastRead = sub->docFreqBlockLastRead;
  register int blockEnd = sub->docFreqBlockEnd;

  if (scores == 0) {
    while (nextDocID < endDoc) {
      //printf("  docID=%d\n", nextDocID);
      int slot = nextDocID & MASK;

      if (docIDs[slot] == nextDocID) {
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
  } else {

    //printf("orMustChunk: nextDocID=%d endDoc=%d\n", nextDocID, endDoc);
    while (nextDocID < endDoc) {
      //printf("  docID=%d\n", nextDocID);
      int slot = nextDocID & MASK;
      if (docIDs[slot] == nextDocID && coords[slot] == prevMustClauseCount) {
        int freq = freqs[blockLastRead];
        double score;
        if (freq < TERM_SCORES_CACHE_SIZE) {
          score = tsCache[freq];
        } else {
          score = sqrt(freq) * termWeight;
        }
        scores[slot] += score;
        coords[slot]++;
        //printf("    keep coord=%d\n", coords[slot]);
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
}

int booleanQueryShouldMustMustNot(PostingsState* subs,
                                  unsigned char *liveDocsBytes,
                                  double **termScoreCache,
                                  float *termWeights,
                                  register int maxDoc,
                                  register int topN,
                                  register int numScorers,
                                  register int docBase,
                                  register int numMust,
                                  register int numMustNot,
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

    // MUST_NOT:
    for(int i=0;i<numMustNot;i++) {
      orMustNotChunk(&subs[i], endDoc, docIDs, coords);
    }

    // MUST:
    int numFilled;
    if (liveDocsBytes != 0) {
      numFilled = orFirstMustChunkWithDeletes(&subs[numMustNot], termScoreCache[numMustNot], termWeights[numMustNot], endDoc, filled, docIDs, scores, coords, liveDocsBytes);
    } else {
      numFilled = orFirstMustChunk(&subs[numMustNot], termScoreCache[numMustNot], termWeights[numMustNot], endDoc, filled, docIDs, scores, coords);
    }
    //printf("numFilled=%d\n", numFilled);
    for(int i=numMustNot+1;i<numMustNot + numMust;i++) {
      orMustChunk(&subs[i], termScoreCache[i], termWeights[i], endDoc, docIDs, scores, coords, i-numMustNot);
    }

    if (topScores != 0) {
      // SHOULD
      for(int i=numMustNot + numMust;i<numScorers;i++) {
        orShouldChunk(&subs[i], termScoreCache[i], termWeights[i], endDoc, docIDs, scores, coords, numMust);
      }
    }

    hitCount += numFilled;

    if (topScores == 0) {
      for(int i=0;i<numFilled;i++) {
        int slot = filled[i];
        if (coords[slot] < numMust) {
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
        if (coords[slot] < numMust) {
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
