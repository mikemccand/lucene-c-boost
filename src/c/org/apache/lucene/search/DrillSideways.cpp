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

#include "common.h"

// Collects one CHUNK of docs into facet bitsets; ported
// from DrillSidewaysScorer.doUnionScoring:
void drillSidewaysCollect(unsigned int topN,
                          unsigned int docBase,
                          int *topDocIDs,
                          float *topScores,
                          unsigned int *filled,
                          int numFilled,
                          int *docIDs,
                          float *scores,
                          unsigned int *counts,
                          unsigned int *missingDims,
                          unsigned int docUpto,
                          PostingsState *subs,
                          int numDims,
                          unsigned int *termsPerDim,
                          unsigned int *totalHits,
                          unsigned long *hitBits,
                          unsigned long **nearMissBits) {

  int subUpto = 0;

  // nocommit should we sometimes do baseScorer "after"?  ie
  // if drill downs are very restrictive...
  unsigned int endDoc = docUpto + CHUNK;

  // First drill-down dim, basically adds SHOULD onto
  // the baseQuery:
  for(int i=0;i<termsPerDim[0];i++) {
    PostingsState *sub = subs + subUpto++;
    int nextDocID = sub->nextDocID;
    int blockLastRead = sub->docFreqBlockLastRead;
    int blockEnd = sub->docFreqBlockEnd;
    unsigned int *docDeltas = sub->docDeltas;      

    while (nextDocID < endDoc) {
      int slot = nextDocID & MASK;
      if (docIDs[slot] == nextDocID) {
        missingDims[slot] = 1;
        counts[slot] = 2;
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

  // Other dims:
  for(int dim=1;dim<numDims;dim++) {
    for(int i=0;i<termsPerDim[dim];i++) {
      PostingsState *sub = subs + subUpto++;
      int nextDocID = sub->nextDocID;
      int blockLastRead = sub->docFreqBlockLastRead;
      int blockEnd = sub->docFreqBlockEnd;
      unsigned int *docDeltas = sub->docDeltas;      

      while (nextDocID < endDoc) {
        int slot = nextDocID & MASK;
        if (docIDs[slot] == nextDocID && counts[slot] >= dim) {
          // This doc is still in the running...
          // TODO: single-valued dims will always be true
          // below; we could somehow specialize
          if (missingDims[slot] >= dim) {
            //if (DEBUG) {
            //  System.out.println("      set docID=" + docID + " count=" + (dim+2));
            //}
            missingDims[slot] = dim+1;
            counts[slot] = dim+2;
          } else {
            //if (DEBUG) {
            //  System.out.println("      set docID=" + docID + " missing count=" + (dim+1));
            //}
            counts[slot] = dim+1;
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

      sub->nextDocID = nextDocID;
      sub->docFreqBlockLastRead = blockLastRead;
    }
  }

  // Collect:
  int docChunkBase = docBase + docUpto;
  for(int i=0;i<numFilled;i++) {
    unsigned int slot = filled[i];
    unsigned int docID = docUpto + slot;
    unsigned int topDocID = docChunkBase + slot;
    if (counts[slot] == 1+numDims) {
      // A real hit
      setLongBit(hitBits, docID);
      for(int j=0;j<numDims;j++) {
        setLongBit(nearMissBits[j], docID);
      }
      (*(totalHits))++;
      if (scores != 0) {
        float score = scores[slot];
        if (score > topScores[1] || (score == topScores[1] && topDocID < topDocIDs[1])) {
          // Hit is competitive   
          topDocIDs[1] = topDocID;
          topScores[1] = score;

          downHeap(topN, topDocIDs, topScores);
        }
      } else {
        // nocommit we can be much more efficient about this
        // ... once queue is full we shouldn't check anymore:
        if (topDocID < topDocIDs[1]) {
          // Hit is competitive   
          topDocIDs[1] = topDocID;

          downHeapNoScores(topN, topDocIDs);
        }
      }
    } else if (counts[slot] == numDims) {
      unsigned int dim = missingDims[slot];
      (*(totalHits+dim+1))++;
      setLongBit(nearMissBits[dim], docID);
    }
  }
}
