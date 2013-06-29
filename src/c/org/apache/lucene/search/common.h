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

#define BLOCK_SIZE 128

#define TERM_SCORES_CACHE_SIZE 32

#define CHUNK 1024
#define MASK (CHUNK-1)

#define NO_MORE_DOCS 2147483647

typedef struct {
  bool docsOnly;

  // How many docs left
  int docsLeft;

  // Where (mapped in RAM) we decode docs/freqs from:
  unsigned char* docFreqs;

  // Current doc/freq block:
  unsigned int *docDeltas;
  unsigned int *freqs;
  int nextDocID;
  int docFreqBlockLastRead;
  int docFreqBlockEnd;

  // Where (mapped in RAM) we decode positions from:
  unsigned char *pos;

  // How many positions left
  long posLeft;

  // Current block of position deltas:
  unsigned int *posDeltas;
  int posBlockLastRead;
  int posBlockEnd;
  //int nextPos;
  long posUpto;

  bool indexHasPayloads;
  bool indexHasOffsets;

  // Used only by ExactPhraseQuery
  unsigned long tfSum;
  unsigned long *tfSums;
  unsigned int *tfs;
  int posLeftInDoc;
  unsigned int nextPos;

  int id;
} PostingsState;

// exported from common.cpp:
void nextDocFreqBlock(PostingsState* sub);
void nextPosBlock(PostingsState* sub);
void skipPositions(PostingsState *sub, long posCount);

void downHeapNoScores(int heapSize, int *topDocIDs);
void downHeap(int heapSize, int *topDocIDs, float *topScores);

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
                           unsigned long **dsNearMissBits);

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
                              unsigned long **dsNearMissBits);


int booleanQueryShouldMust(PostingsState* subs,
                           unsigned char *liveDocsBytes,
                           double **termScoreCache,
                           float *termWeights,
                           int maxDoc,
                           int topN,
                           int numScorers,
                           int docBase,
                           int numMust,
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
                           unsigned long **dsNearMissBits);

int booleanQueryShouldMustMustNot(PostingsState* subs,
                                  unsigned char *liveDocsBytes,
                                  double **termScoreCache,
                                  float *termWeights,
                                  int maxDoc,
                                  int topN,
                                  int numScorers,
                                  int docBase,
                                  int numMust,
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
                                  PostingsState *dsSubs,
                                  unsigned int *dsCounts,
                                  unsigned int *dsMissingDims,
                                  unsigned int dsNumDims,
                                  unsigned int *dsTotalHits,
                                  unsigned int *dsTermsPerDim,
                                  unsigned long *dsHitBits,
                                  unsigned long **dsNearMissBits);

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
                int *posOffsets);

unsigned int drillSidewaysCollect(unsigned int topN,
                                  unsigned int docBase,
                                  int *topDocIDs,
                                  float *topScores,
                                  float *normTable,
                                  unsigned char *norms,
                                  float *coordFactors,
                                  unsigned int *coords,
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
                                  unsigned long **nearMissBits);

bool isSet(unsigned char *bits, unsigned int docID);
void setLongBit(unsigned long *bits, unsigned int docID);

//#define DEBUG

#ifndef DEBUG
#define printf NO
#define fflush NO
#endif

