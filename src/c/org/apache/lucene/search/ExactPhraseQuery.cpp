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
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "common.h"

static void
orFirstMustChunk(PostingsState *sub,
                 int endDoc,
                 int *docIDs,
                 unsigned int *coords) {
#ifdef DEBUG
  printf("first must\n");
#endif
  int nextDocID = sub->nextDocID;
  unsigned int *docDeltas = sub->docDeltas;
  unsigned int *freqs = sub->freqs;

  int blockLastRead = sub->docFreqBlockLastRead;
  int blockEnd = sub->docFreqBlockEnd;

  long tfSum = sub->tfSum;
  unsigned long *tfSums = sub->tfSums;
  unsigned int *tfs = sub->tfs;

  // First scorer is different because we know slot is
  // "new" for every hit:
  while (nextDocID < endDoc) {
    int slot = nextDocID & MASK;
    docIDs[slot] = nextDocID;
    coords[slot] = 1;
    unsigned int freq = freqs[blockLastRead];
#ifdef DEBUG
    printf("  docID=%d freq=%d tfSum=%d\n", nextDocID, freq, tfSum);
#endif
    tfs[slot] = freq;
    tfSums[slot] = tfSum;
    tfSum += freq;

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
  sub->tfSum = tfSum;
  sub->nextDocID = nextDocID;
  sub->docFreqBlockLastRead = blockLastRead;
  //printf("return numFilled=%d\n", numFilled);
}

static void
orFirstMustChunkWithDeletes(PostingsState *sub,
                            int endDoc,
                            int *docIDs,
                            unsigned int *coords,
                            unsigned char *liveDocsBytes) {

  int nextDocID = sub->nextDocID;
  unsigned int *docDeltas = sub->docDeltas;
  unsigned int *freqs = sub->freqs;

  int blockLastRead = sub->docFreqBlockLastRead;
  int blockEnd = sub->docFreqBlockEnd;

  long tfSum = sub->tfSum;
  unsigned long *tfSums = sub->tfSums;
  unsigned int *tfs = sub->tfs;

  // First scorer is different because we know slot is
  // "new" for every hit:
  while (nextDocID < endDoc) {
    unsigned int freq = freqs[blockLastRead];
    if (isSet(liveDocsBytes, nextDocID)) {
      //printf("  docID=%d\n", nextDocID);
      int slot = nextDocID & MASK;
      docIDs[slot] = nextDocID;
      tfSums[slot] = tfSum;
      tfs[slot] = freq;
      coords[slot] = 1;
    }
    tfSum += freq;

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
  sub->tfSum = tfSum;
  sub->nextDocID = nextDocID;
  sub->docFreqBlockLastRead = blockLastRead;
}

static void
orMustChunk(PostingsState *sub,
            int endDoc,
            int *docIDs,
            unsigned int *coords,
            int prevMustClauseCount) {

#ifdef DEBUG
  printf("middle must\n");
#endif
  int nextDocID = sub->nextDocID;
  unsigned int *docDeltas = sub->docDeltas;
  unsigned int *freqs = sub->freqs;

  int blockLastRead = sub->docFreqBlockLastRead;
  int blockEnd = sub->docFreqBlockEnd;
  long tfSum = sub->tfSum;
  unsigned long *tfSums = sub->tfSums;
  unsigned int *tfs = sub->tfs;

  while (nextDocID < endDoc) {
    //printf("  docID=%d\n", nextDocID);
    int slot = nextDocID & MASK;
    unsigned int freq = freqs[blockLastRead];
    if (docIDs[slot] == nextDocID && coords[slot] == prevMustClauseCount) {
      coords[slot]++;
      tfSums[slot] = tfSum;
      tfs[slot] = freq;
    }

    tfSum += freq;

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

  sub->tfSum = tfSum;
  sub->nextDocID = nextDocID;
  sub->docFreqBlockLastRead = blockLastRead;
}

static int
orLastMustChunk(PostingsState *sub,
                int endDoc,
                unsigned int *filled,
                int *docIDs,
                unsigned int *coords,
                int prevMustClauseCount) {

  int nextDocID = sub->nextDocID;
  unsigned int *docDeltas = sub->docDeltas;
  unsigned int *freqs = sub->freqs;

  int blockLastRead = sub->docFreqBlockLastRead;
  int blockEnd = sub->docFreqBlockEnd;
  long tfSum = sub->tfSum;
  unsigned long *tfSums = sub->tfSums;
  unsigned int *tfs = sub->tfs;
  int numFilled = 0;

#ifdef DEBUG
  printf("last must\n");
#endif

  while (nextDocID < endDoc) {
    int slot = nextDocID & MASK;
    unsigned int freq = freqs[blockLastRead];
#ifdef DEBUG
    printf("  docID=%d freq=%d\n", nextDocID, freq);
#endif
    if (docIDs[slot] == nextDocID && coords[slot] == prevMustClauseCount) {
      coords[slot]++;
      tfSums[slot] = tfSum;
      tfs[slot] = freq;
      filled[numFilled++] = slot;
    }

    tfSum += freq;

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

  sub->tfSum = tfSum;
  sub->nextDocID = nextDocID;
  sub->docFreqBlockLastRead = blockLastRead;

  return numFilled;
}

// How many positions we process in each chunk:
#define POS_CHUNK 1024
#define POS_MASK (POS_CHUNK-1)

int phraseQuery(PostingsState* subs,
                unsigned char *liveDocsBytes,
                double *termScoreCache,
                float termWeight,
                int maxDoc,
                int topN,
                int numScorers,
                int docBase,
                unsigned int *filled,
                int *docIDs,
                unsigned int *coords,
                float *topScores,
                int *topDocIDs,
                float *normTable,
                unsigned char *norms,
                int *posOffsets) {

  bool failed = false;
  unsigned int *posCounts = 0;
  unsigned int *positions = 0;
  int docUpto = 0;
  int hitCount = 0;
  int countUpto = 1;
  float scoreToBeat = 0.0;

  posCounts = (unsigned int *) calloc(POS_CHUNK, sizeof(int));
  if (posCounts == 0) {
    failed = true;
    goto end;
  }

  for(int i=0;i<numScorers;i++) {
    subs[i].tfSums = (unsigned long *) malloc(CHUNK * sizeof(long));
    if (subs[i].tfSums == 0) {
      failed = true;
      goto end;
    }
    subs[i].tfs = (unsigned int *) malloc(CHUNK * sizeof(int));
    if (subs[i].tfs == 0) {
      failed = true;
      goto end;
    }
  }

  while (docUpto < maxDoc) {
#ifdef DEBUG
    printf("cycle docUpto=%d\n", docUpto);
#endif
    int endDoc = docUpto + CHUNK;
    if (liveDocsBytes != 0) {
      orFirstMustChunkWithDeletes(&subs[0], endDoc, docIDs, coords, liveDocsBytes);
    } else {
      orFirstMustChunk(&subs[0], endDoc, docIDs, coords);
    }
    for(int i=1;i<numScorers-1;i++) {
      orMustChunk(&subs[i], endDoc, docIDs, coords, i);
    }
    // TODO: we could check positions inside orLastMustChunk
    // instead, saves one pass:
    int numFilled = orLastMustChunk(&subs[numScorers-1], endDoc, filled, docIDs, coords, numScorers-1);
#ifdef DEBUG
    printf("  numFilled=%d\n", numFilled);
#endif

    int docChunkBase = docBase + docUpto;

    for(int fill=0;fill<numFilled;fill++) {
      int slot = filled[fill];
#ifdef DEBUG
      printf("\ncheck positions slot=%d docID=%d\n", slot, docIDs[slot]);
#endif
      //printf("    docID=%d\n",
      //docIDs[slot]);fflush(stdout);

      // This doc has all of the terms, now check their
      // positions:
        
      // Seek/init pos so we are positioned at posDeltas
      // for this document:
      unsigned int minTF = 4294967295;
      for(int j=0;j<numScorers;j++) {
        PostingsState *sub = subs+j;
#ifdef DEBUG
        printf("  scorer[%d]: skip %d positions, %d pos in doc, tfSum=%d, posBlockLastRead=%d\n",
               j, sub->tfSums[slot] - sub->posUpto, sub->tfs[slot], sub->tfSums[slot],
               sub->posBlockLastRead);
#endif
        skipPositions(sub, sub->tfSums[slot]);
#ifdef DEBUG
        printf("    posBlockLastRead=%d posDelta=%d nextPosDelta=%d\n", sub->posBlockLastRead, sub->posDeltas[sub->posBlockLastRead],
               sub->posDeltas[sub->posBlockLastRead+1]);
#endif
        sub->nextPos = posOffsets[j] + sub->posDeltas[sub->posBlockLastRead];
#ifdef DEBUG
        printf("    nextPos=%d\n", sub->nextPos);
#endif
        if (sub->tfs[slot] < minTF) {
          minTF = sub->tfs[slot];
        }
        sub->posLeftInDoc = sub->tfs[slot];
#ifdef DEBUG
        printf("    posLeftInDoc=%d posUpto=%d\n", sub->posLeftInDoc, sub->posUpto);
#endif
      }

      bool doStopAfterFirstPhrase;
      bool doSkipCollect;
      if (topScores == 0) {
        doStopAfterFirstPhrase = true;
        doSkipCollect = false;
      } else {
        // Invert the score of the bottom of the queue:
        float f = (scoreToBeat / normTable[norms[docIDs[slot]]]) / termWeight;
        unsigned int phraseFreqToBeat = (unsigned int) (f*f);
        //printf("scoreToBeat=%g phraseFreqToBeat=%d\n", scoreToBeat, phraseFreqToBeat);
        doStopAfterFirstPhrase = doSkipCollect = minTF < phraseFreqToBeat;
        //if (doStopAfterFirstPhrase) {
        //printf("do stop: %d vs %d\n", minTF, phraseFreqToBeat);
        //}
      }

      bool done = false;

      int phraseFreq = 0;

      // Find all phrase matches, in windows of POS_CHUNK:

      int posUpto = 0;

      while (true) {
#ifdef DEBUG
        printf("  cycle posUpto=%d\n", posUpto);
#endif
        if (countUpto + numScorers + 1 < countUpto) {
          // Wrap-around
          countUpto = 1;
          memset(posCounts, 0, POS_CHUNK*sizeof(int));
        }
        PostingsState *sub = subs;

        int endPos = posUpto + POS_CHUNK;

        // First terms in phrase:
        int posBlockLastRead = sub->posBlockLastRead;
        unsigned int *posDeltas = sub->posDeltas;
        unsigned int posBlockEnd = sub->posBlockEnd;
        int pos = sub->nextPos;

        while (pos < endPos) {
          int posSlot = pos & POS_MASK;
          posCounts[posSlot] = countUpto;
#ifdef DEBUG
          printf("scorer[0] nextPos=%d\n", pos);
#endif

          if (--sub->posLeftInDoc == 0) {
#ifdef DEBUG
            printf("  done: no more positions\n");
#endif
            posBlockLastRead++;
            done = true;
            break;
          }

          if (posBlockLastRead == posBlockEnd) {
#ifdef DEBUG
            printf("  nextPosBlock: posBlockEnd=%d\n", posBlockEnd);
#endif
            nextPosBlock(sub);
            posBlockLastRead = -1;
            posBlockEnd = sub->posBlockEnd;
          }
#ifdef DEBUG
          printf("  add posDeltas[%d]=%d\n", 1+posBlockLastRead,
                 posDeltas[1+posBlockLastRead]);fflush(stdout);
#endif
          pos += posDeltas[++posBlockLastRead];
        }

        sub->posBlockLastRead = posBlockLastRead;
        sub->nextPos = pos;

        // Middle terms in phrase:
        for(int i=1;i<numScorers-1;i++) {
          sub = subs + i;
          posBlockLastRead = sub->posBlockLastRead;
          posDeltas = sub->posDeltas;
          posBlockEnd = sub->posBlockEnd;
          pos = sub->nextPos;

          while (pos < endPos) {
#ifdef DEBUG
            printf("scorer[%d] nextPos=%d\n", i, pos);
#endif
            if (pos >= 0) {
              int posSlot = pos & POS_MASK;
              if (posCounts[posSlot] == countUpto) {
                posCounts[posSlot]++;
              }
            }
            if (--sub->posLeftInDoc == 0) {
              posBlockLastRead++;
              done = true;
              break;
            }
            if (posBlockLastRead == posBlockEnd) {
              nextPosBlock(sub);
              posBlockLastRead = -1;
              posBlockEnd = sub->posBlockEnd;
            }
            pos += posDeltas[++posBlockLastRead];
          }
          sub->posBlockLastRead = posBlockLastRead;
          sub->nextPos = pos;
          countUpto++;
        }

        // Last term in phrase:
        sub = subs + (numScorers-1);
        posBlockLastRead = sub->posBlockLastRead;
        posDeltas = sub->posDeltas;
        posBlockEnd = sub->posBlockEnd;
        pos = sub->nextPos;

        while (pos < endPos) {
#ifdef DEBUG
          printf("scorer[%d] nextPos=%d\n", numScorers-1, pos);fflush(stdout);
#endif
          if (pos >= 0) {
            int posSlot = pos & POS_MASK;
            if (posCounts[posSlot] == countUpto) {
#ifdef DEBUG
              printf("  match pos=%d slot=%d\n", pos, posSlot);fflush(stdout);
#endif
              phraseFreq++;
              if (doStopAfterFirstPhrase) {
                // ConstantScoreQuery(PhraseQuery), so we can
                // stop & collect hit as soon as we find one
                // phrase match
                done = true;
                break;
              }
            }
          }

          if (--sub->posLeftInDoc == 0) {
            posBlockLastRead++;
            done = true;
            break;
          }

          if (posBlockLastRead == posBlockEnd) {
#ifdef DEBUG
            printf("  nextPosBlock: posBlockEnd=%d\n", posBlockEnd);
#endif
            nextPosBlock(sub);
            posBlockLastRead = -1;
            posBlockEnd = sub->posBlockEnd;
          }
#ifdef DEBUG
          printf("  add posDeltas[%d]=%d\n", 1+posBlockLastRead, posDeltas[1+posBlockLastRead]);fflush(stdout);
#endif
          pos += posDeltas[++posBlockLastRead];
        }

        countUpto++;
        sub->posBlockLastRead = posBlockLastRead;
        sub->nextPos = pos;

        if (done) {
          break;
        }

        posUpto += POS_CHUNK;
      }

      for(int j=0;j<numScorers;j++) {
        PostingsState *sub = subs+j;
#ifdef DEBUG
        printf("add posUpto[%d]: %d; posLeftInDoc %d\n", j, sub->tfs[slot] - sub->posLeftInDoc, sub->posLeftInDoc);
#endif
        sub->posUpto += sub->tfs[slot] - sub->posLeftInDoc;
      }

      if (phraseFreq != 0) {
        //printf("      freq=%d\n", phraseFreq);fflush(stdout);

#ifdef DEBUG
        printf("  phraseFreq=%d topScores=%lx\n", phraseFreq, topScores);fflush(stdout);
#endif

        hitCount++;

        if (doSkipCollect) {
          continue;
        }

        int docID = docChunkBase + slot;

#ifdef DEBUG
        printf("  now collect\n");fflush(stdout);
#endif

        // collect
        if (topScores == 0) {

          // TODO: we can stop collecting, and tracking filled,
          // after chunk once queue is full
          if (docID < topDocIDs[1]) {
            // Hit is competitive   
            topDocIDs[1] = docID;
            downHeapNoScores(topN, topDocIDs);
          }
        } else {
          float score;
          if (phraseFreq < TERM_SCORES_CACHE_SIZE) {
            score = termScoreCache[phraseFreq];
          } else {
            score = sqrt(phraseFreq) * termWeight;
          }

          score *= normTable[norms[docIDs[slot]]];

          if (score > topScores[1] || (score == topScores[1] && docID < topDocIDs[1])) {
            // Hit is competitive   
            topDocIDs[1] = docID;
            topScores[1] = score;

            downHeap(topN, topDocIDs, topScores);
#ifdef DEBUG
            printf("    ** score=%g phraseFreq=%d norm=%g\n", score, phraseFreq, normTable[norms[docIDs[slot]]]);fflush(stdout);
#endif

            scoreToBeat = topScores[1];
          }
        }
      }
    }

    docUpto += CHUNK;
  }

 end:
  if (posCounts != 0) {
    free(posCounts);
  }
  for(int i=0;i<numScorers;i++) {
    if (subs[i].tfSums != 0) {
      free(subs[i].tfSums);
    }
    if (subs[i].tfs != 0) {
      free(subs[i].tfs);
    }
  }

  if (failed) {
    return -1;
  } else {
    return hitCount;
  }
}
