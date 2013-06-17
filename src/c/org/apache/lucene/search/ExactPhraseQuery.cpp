static void
orFirstMustChunk(PostingsState *sub,
                 register int endDoc,
                 register int *docIDs,
                 register unsigned int *coords) {

  register int nextDocID = sub->nextDocID;
  register unsigned int *docDeltas = sub->docDeltas;
  register unsigned int *freqs = sub->freqs;

  register int blockLastRead = sub->blockLastRead;
  register int blockEnd = sub->blockEnd;

  register long tfSum = sub->tfSum;
  register unsigned long *tfSums = sub->tfSums;
  register unsigned int *tfs = sub->tfs;

  // First scorer is different because we know slot is
  // "new" for every hit:
  while (nextDocID < endDoc) {
    //printf("  docID=%d\n", nextDocID);
    int slot = nextDocID & MASK;
    docIDs[slot] = nextDocID;
    tfSums[slot] = tfSum;
    unsigned int freq = freqs[blockLastRead];
    tfs[slot] = freq;
    coords[slot] = 1;
    tfSum += freq;

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
  sub->tfSum = tfSum;
  sub->nextDocID = nextDocID;
  sub->blockLastRead = blockLastRead;
  //printf("return numFilled=%d\n", numFilled);
}

static void
orFirstMustChunkWithDeletes(PostingsState *sub,
                            register int endDoc,
                            register int *docIDs,
                            register float *scores,
                            register unsigned int *coords,
                            register unsigned char *liveDocsBytes) {

  register int nextDocID = sub->nextDocID;
  register unsigned int *docDeltas = sub->docDeltas;
  register unsigned int *freqs = sub->freqs;

  register int blockLastRead = sub->blockLastRead;
  register int blockEnd = sub->blockEnd;

  register long tfSum = sub->tfSum;
  register unsigned long *tfSums = sub->tfSums;
  register unsigned int *tfs = sub->tfs;

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
        nextBlock(sub);
        blockLastRead = -1;
        blockEnd = sub->blockEnd;
      }
    }
    nextDocID += docDeltas[++blockLastRead];
  }
  sub->tfSum = tfSum;
  sub->nextDocID = nextDocID;
  sub->blockLastRead = blockLastRead;
}

static void
orMustChunk(PostingsState *sub,
            register int endDoc,
            register int *docIDs,
            register unsigned int *coords,
            register int prevMustClauseCount) {

  register int nextDocID = sub->nextDocID;
  register unsigned int *docDeltas = sub->docDeltas;
  register unsigned int *freqs = sub->freqs;

  register int blockLastRead = sub->blockLastRead;
  register int blockEnd = sub->blockEnd;
  register long tfSum = sub->tfSum;
  register unsigned long *tfSums = sub->tfSums;
  register unsigned int *tfs = sub->tfs;

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
        nextBlock(sub);
        blockLastRead = -1;
        blockEnd = sub->blockEnd;
      }
    }
    nextDocID += docDeltas[++blockLastRead];
  }

  sub->tfSum = tfSum;
  sub->nextDocID = nextDocID;
  sub->blockLastRead = blockLastRead;
}

static int
orLastMustChunk(PostingsState *sub,
                register int endDoc,
                register unsigned int *filled,
                register int *docIDs,
                register unsigned int *coords,
                register int prevMustClauseCount) {

  register int nextDocID = sub->nextDocID;
  register unsigned int *docDeltas = sub->docDeltas;
  register unsigned int *freqs = sub->freqs;

  register int blockLastRead = sub->blockLastRead;
  register int blockEnd = sub->blockEnd;
  register long tfSum = sub->tfSum;
  register unsigned long *tfSums = sub->tfSums;
  register unsigned int *tfs = sub->tfs;
  register int numFilled = 0;

  while (nextDocID < endDoc) {
    //printf("  docID=%d\n", nextDocID);
    int slot = nextDocID & MASK;
    unsigned int freq = freqs[blockLastRead];
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
        nextBlock(sub);
        blockLastRead = -1;
        blockEnd = sub->blockEnd;
      }
    }
    nextDocID += docDeltas[++blockLastRead];
  }

  sub->tfSum = tfSum;
  sub->nextDocID = nextDocID;
  sub->blockLastRead = blockLastRead;

  return numFilled;
}

// How many positions we process in each chunk:
#define POS_CHUNK 1024
#define POS_MASK (POS_CHUNK-1)

int phraseQuery(PostingsState* subs,
                unsigned char *liveDocsBytes,
                double *termScoreCache,
                float termWeight,
                register int maxDoc,
                register int topN,
                register int numScorers,
                register int docBase,
                register unsigned int *filled,
                register int *docIDs,
                register unsigned int *coords,
                register float *topScores,
                register int *topDocIDs,
                register float *normTable,
                register unsigned char *norms) {

  boolean failed = false;
  unsigned int *posCounts = 0;
  unsigned int *positions = 0;

  posCounts = (unsigned int *) malloc(POS_CHUNK * sizeof(int));
  if (posCounts == 0) {
    failed = true;
    goto end;
  }

  positions = (unsigned int *) malloc(POS_CHUNK * sizeof(int));
  if (positions == 0) {
    failed = true;
    goto end;
  }

  for(int i=0;i<POS_CHUNK;i++) {
    positions[i] = -1;
  }

  for(int i=0;i<numScorers;i++) {
    subs[i].tfSums = (unsigned long *) malloc(POS_CHUNK * sizeof(long));
    if (subs[i].tfSums == 0) {
      failed = true;
      goto end;
    }
    subs[i].tfs = (unsigned int *) malloc(POS_CHUNK * sizeof(int));
    if (subs[i].tfs == 0) {
      failed = true;
      goto end;
    }
  }

  int docUpto = 0;
  int hitCount = 0;
  while (docUpto < maxDoc) {
    if (liveDocsBytes != 0) {
      orFirstMustChunkWithDeletes(&subs[0], endDoc, docIDs, coords, liveDocsBytes);
    } else {
      orFirstMustChunk(&subs[0], endDoc, docIDs, coords);
    }
    for(int i=1;i<numScorers-1;i++) {
      orMustChunk(&subs[i], endDoc, docIDs, coords);
    }
    numFilled = orLastMustChunk(&subs[numScorers-1], endDoc, filled, docIDs, coords);

    for(int i=0;i<numFilled;i++) {
      int slot = filled[i];
      // This doc has all of the terms, now check their
      // positions:
        
      // Seek/init pos so we are positioned at posDeltas
      // for this document:
      for(int j=0;j<numScorers;j++) {
        skipPositions(sub, sub->tfSums[slot]);
        sub->nextPos = -posShift[j];
      }

      bool done = false;

      int phraseFreq = 0;

      // Find all phrase matches, in windows of POS_CHUNK:

      int posUpto = 0;

      while (true) {

        PostingsState *sub = subs;

        int endPos = posUpto + POS_CHUNK;

        int posBlockLastRead = sub->posBlockLastRead;
        unsigned int *posDeltas = sub->posDeltas;
        unsigned int posBlockEnd = sub->posBlockEnd;
        int pos = sub->nextPos;

        while (pos < endPos) {
          int posSlot = pos & POS_MASK;
          positions[posSlot] = pos;
          posCounts[posSlot] = 1;

          if (posBlockLastRead = posBlockEnd) {
            if (sub->posLeft = 0) {
              done = true;
              break;
            } else {
              nextPosBlock(sub);
              posBlockLastRead = -1;
              posBlockEnd = sub->posBlockEnd;
            }
          }
        }
        sub->posBlockEnd = posBlockEnd;
        sub->posBlockLastRead = posBlockLastRead;
        sub->nextPos = pos;

        for(int i=1;i<numScorers-1;i++) {
          PostingsState *sub = subs + i;
          int posBlockLastRead = sub->posBlockLastRead;
          unsigned int *posDeltas = sub->posDeltas;
          unsigned int posBlockEnd = sub->posBlockEnd;
          int pos = sub->nextPos;

          while (pos < endPos) {
            int posSlot = pos & POS_MASK;
            if (positions[posSlot] == pos) {
              posCounts[posSlot]++;
            }

            if (posBlockLastRead = posBlockEnd) {
              if (sub->posLeft = 0) {
                done = true;
                break;
              } else {
                nextPosBlock(sub);
                posBlockLastRead = -1;
                posBlockEnd = sub->posBlockEnd;
              }
            }
          }
          sub->posBlockEnd = posBlockEnd;
          sub->posBlockLastRead = posBlockLastRead;
          sub->nextPos = pos;
        }

        PostingsState *sub = subs + i;
        int posBlockLastRead = sub->posBlockLastRead;
        unsigned int *posDeltas = sub->posDeltas;
        unsigned int posBlockEnd = sub->posBlockEnd;
        int pos = sub->nextPos;

        while (pos < endPos) {
          int posSlot = pos & POS_MASK;
          if (positions[posSlot] == pos && posCounts[posSlot] == numScorers-1) {
            phraseFreq++;
            if (topScores == 0) {
              // ConstantScoreQuery(PhraseQuery), so we can
              // stop & collect hit as soon as we find one
              // phrase match
              done = true;
              break;
            }
          }

          if (posBlockLastRead = posBlockEnd) {
            if (sub->posLeft = 0) {
              done = true;
              break;
            } else {
              nextPosBlock(sub);
              posBlockLastRead = -1;
              posBlockEnd = sub->posBlockEnd;
            }
          }
        }
        sub->posBlockEnd = posBlockEnd;
        sub->posBlockLastRead = posBlockLastRead;
        sub->nextPos = pos;

        if (done) {
          break;
        }

        posUpto += POS_CHUNK;
      }

      if (phraseFreq != 0) {

        hitCount++;

        int docID = docBase + docIDs[slot];

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
            score = termScoreCache[freq];
          } else {
            score = sqrt(freq) * termWeight;
          }

          score *= normTable[norms[docIDs[slot]]];

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

 end:
  if (positions != 0) {
    free(positions);
  }
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


