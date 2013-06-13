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

#define CHUNK 2048
#define MASK (CHUNK-1)

#define NO_MORE_DOCS 2147483647

typedef struct {
  bool docsOnly;

  // How many docs left
  int docsLeft;

  // Where (mapped in RAM) we decode postings from:
  unsigned char* p;

  // Current block
  unsigned int *docDeltas;
  unsigned int *freqs;
  int nextDocID;
  int blockLastRead;
  int blockEnd;
  int id;
} PostingsState;

// exported from decode.cpp:
void nextBlock(PostingsState* sub);
void downHeapNoScores(int heapSize, int *topDocIDs);
void downHeap(int heapSize, int *topDocIDs, float *topScores);

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
                           register unsigned char *norms);

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
                              register unsigned char *skips);

int booleanQueryShouldMust(PostingsState* subs,
                           unsigned char *liveDocsBytes,
                           double **termScoreCache,
                           float *termWeights,
                           register int maxDoc,
                           register int topN,
                           register int numScorers,
                           register int docBase,
                           register int numMust,
                           register unsigned int *filled,
                           register int *docIDs,
                           register float *scores,
                           register unsigned int *coords,
                           register float *topScores,
                           register int *topDocIDs,
                           register float *coordFactors,
                           register float *normTable,
                           register unsigned char *norms);

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
                                  register unsigned char *norms);

bool isSet(unsigned char *bits, unsigned int docID);
