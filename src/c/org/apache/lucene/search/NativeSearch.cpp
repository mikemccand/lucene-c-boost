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

//
// To see assembly:
//
//   g++ -fpermissive -S -O4 -o test.s -I/usr/local/src/jdk1.6.0_32/include -I/usr/local/src/jdk1.6.0_32/include/linux /l/nativebq/lucene/misc/src/java/org/apache/lucene/search/NativeSearch.cpp
//

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/mman.h>
#include <errno.h>
#include <jni.h>
#include <math.h> // for sqrt
#include <stdlib.h> // malloc
#include <string.h> // memcpy
#include <byteswap.h>

#define BLOCK_SIZE 128
#define NO_MORE_DOCS 2147483647

#define CHUNK 2048
#define MASK (CHUNK-1)

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

typedef struct {
  int docID;
  double score;
  int coord;
} Bucket;

//static FILE *fp = fopen("logout.txt", "w");

bool isSet(unsigned char *bits, unsigned int docID) {
  bool x = (bits[docID >> 3] & (1 << (docID & 7))) != 0;
  //fprintf(fp, "isSet docID=%d ret=%d\n", docID, x);fflush(fp);
  return x;
}

// Returns 1 if the bit was not previously set
inline static void setLongBit(long *bits, int index) {
  int wordNum = index >> 6;      // div 64
  int bit = index & 0x3f;     // mod 64
  long bitmask = 1L << bit;
  bits[wordNum] |= bitmask;
}

static int getLongBit(long *bits, int index) {
  int wordNum = index >> 6;      // div 64
  int bit = index & 0x3f;     // mod 64
  long bitmask = 1L << bit;
  return (bits[wordNum] & bitmask) != 0;
}

static unsigned char readByte(PostingsState *sub) {
  //printf("readByte p=%ld\n", sub->p);fflush(stdout);
  return *(sub->p++);
}

static unsigned int readVInt(PostingsState *sub) {
  char b = (char) readByte(sub);
  if (b >= 0) return b;
  unsigned int i = b & 0x7F;
  b = readByte(sub);
  i |= (b & 0x7F) << 7;
  if (b >= 0) return i;
  b = readByte(sub);
  i |= (b & 0x7F) << 14;
  if (b >= 0) return i;
  b = readByte(sub);
  i |= (b & 0x7F) << 21;
  if (b >= 0) return i;
  b = readByte(sub);
  // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
  return i | ((b & 0x0F) << 28);
}

static void decodeSingleBlock1(unsigned long *blocks, unsigned int *values) {
  int valuesOffset = 0;
  long mask = 1;
  for(int i=0;i<2;i++) {
    unsigned long block = blocks[i];
    block = __bswap_64(block);
    values[valuesOffset++] = (int) (block & mask);
    for (int j = 1; j < 64; ++j) {
      block >>= 1;
      values[valuesOffset++] = (int) (block & mask);
    }
  }
}

static void decodeSingleBlock2(unsigned long *blocks, unsigned int *values) {
  int valuesOffset = 0;
  long mask = 3;
  for(int i=0;i<4;i++) {
    unsigned long block = blocks[i];
    block = __bswap_64(block);
    values[valuesOffset++] = (int) (block & mask);
    for (int j = 1; j < 32; ++j) {
      block >>= 2;
      values[valuesOffset++] = (int) (block & mask);
    }
  }
}

static void decodeSingleBlock4(unsigned long *blocks, unsigned int *values) {
  int valuesOffset = 0;
  long mask = 15;
  for(int i=0;i<8;i++) {
    unsigned long block = blocks[i];
    block = __bswap_64(block);
    values[valuesOffset++] = (int) (block & mask);
    for (int j = 1; j < 16; ++j) {
      block >>= 4;
      values[valuesOffset++] = (int) (block & mask);
    }
  }
}

// BEGIN AUTOGEN CODE (gen_Packed.py)

static void decode3(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 2; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 61);
    values[valuesOffset++] = (int) ((block0 >> 58) & 7L);
    values[valuesOffset++] = (int) ((block0 >> 55) & 7L);
    values[valuesOffset++] = (int) ((block0 >> 52) & 7L);
    values[valuesOffset++] = (int) ((block0 >> 49) & 7L);
    values[valuesOffset++] = (int) ((block0 >> 46) & 7L);
    values[valuesOffset++] = (int) ((block0 >> 43) & 7L);
    values[valuesOffset++] = (int) ((block0 >> 40) & 7L);
    values[valuesOffset++] = (int) ((block0 >> 37) & 7L);
    values[valuesOffset++] = (int) ((block0 >> 34) & 7L);
    values[valuesOffset++] = (int) ((block0 >> 31) & 7L);
    values[valuesOffset++] = (int) ((block0 >> 28) & 7L);
    values[valuesOffset++] = (int) ((block0 >> 25) & 7L);
    values[valuesOffset++] = (int) ((block0 >> 22) & 7L);
    values[valuesOffset++] = (int) ((block0 >> 19) & 7L);
    values[valuesOffset++] = (int) ((block0 >> 16) & 7L);
    values[valuesOffset++] = (int) ((block0 >> 13) & 7L);
    values[valuesOffset++] = (int) ((block0 >> 10) & 7L);
    values[valuesOffset++] = (int) ((block0 >> 7) & 7L);
    values[valuesOffset++] = (int) ((block0 >> 4) & 7L);
    values[valuesOffset++] = (int) ((block0 >> 1) & 7L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 1L) << 2) | (block1 >> 62));
    values[valuesOffset++] = (int) ((block1 >> 59) & 7L);
    values[valuesOffset++] = (int) ((block1 >> 56) & 7L);
    values[valuesOffset++] = (int) ((block1 >> 53) & 7L);
    values[valuesOffset++] = (int) ((block1 >> 50) & 7L);
    values[valuesOffset++] = (int) ((block1 >> 47) & 7L);
    values[valuesOffset++] = (int) ((block1 >> 44) & 7L);
    values[valuesOffset++] = (int) ((block1 >> 41) & 7L);
    values[valuesOffset++] = (int) ((block1 >> 38) & 7L);
    values[valuesOffset++] = (int) ((block1 >> 35) & 7L);
    values[valuesOffset++] = (int) ((block1 >> 32) & 7L);
    values[valuesOffset++] = (int) ((block1 >> 29) & 7L);
    values[valuesOffset++] = (int) ((block1 >> 26) & 7L);
    values[valuesOffset++] = (int) ((block1 >> 23) & 7L);
    values[valuesOffset++] = (int) ((block1 >> 20) & 7L);
    values[valuesOffset++] = (int) ((block1 >> 17) & 7L);
    values[valuesOffset++] = (int) ((block1 >> 14) & 7L);
    values[valuesOffset++] = (int) ((block1 >> 11) & 7L);
    values[valuesOffset++] = (int) ((block1 >> 8) & 7L);
    values[valuesOffset++] = (int) ((block1 >> 5) & 7L);
    values[valuesOffset++] = (int) ((block1 >> 2) & 7L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 3L) << 1) | (block2 >> 63));
    values[valuesOffset++] = (int) ((block2 >> 60) & 7L);
    values[valuesOffset++] = (int) ((block2 >> 57) & 7L);
    values[valuesOffset++] = (int) ((block2 >> 54) & 7L);
    values[valuesOffset++] = (int) ((block2 >> 51) & 7L);
    values[valuesOffset++] = (int) ((block2 >> 48) & 7L);
    values[valuesOffset++] = (int) ((block2 >> 45) & 7L);
    values[valuesOffset++] = (int) ((block2 >> 42) & 7L);
    values[valuesOffset++] = (int) ((block2 >> 39) & 7L);
    values[valuesOffset++] = (int) ((block2 >> 36) & 7L);
    values[valuesOffset++] = (int) ((block2 >> 33) & 7L);
    values[valuesOffset++] = (int) ((block2 >> 30) & 7L);
    values[valuesOffset++] = (int) ((block2 >> 27) & 7L);
    values[valuesOffset++] = (int) ((block2 >> 24) & 7L);
    values[valuesOffset++] = (int) ((block2 >> 21) & 7L);
    values[valuesOffset++] = (int) ((block2 >> 18) & 7L);
    values[valuesOffset++] = (int) ((block2 >> 15) & 7L);
    values[valuesOffset++] = (int) ((block2 >> 12) & 7L);
    values[valuesOffset++] = (int) ((block2 >> 9) & 7L);
    values[valuesOffset++] = (int) ((block2 >> 6) & 7L);
    values[valuesOffset++] = (int) ((block2 >> 3) & 7L);
    values[valuesOffset++] = (int) (block2 & 7L);
  }
}

static void decode5(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 2; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 59);
    values[valuesOffset++] = (int) ((block0 >> 54) & 31L);
    values[valuesOffset++] = (int) ((block0 >> 49) & 31L);
    values[valuesOffset++] = (int) ((block0 >> 44) & 31L);
    values[valuesOffset++] = (int) ((block0 >> 39) & 31L);
    values[valuesOffset++] = (int) ((block0 >> 34) & 31L);
    values[valuesOffset++] = (int) ((block0 >> 29) & 31L);
    values[valuesOffset++] = (int) ((block0 >> 24) & 31L);
    values[valuesOffset++] = (int) ((block0 >> 19) & 31L);
    values[valuesOffset++] = (int) ((block0 >> 14) & 31L);
    values[valuesOffset++] = (int) ((block0 >> 9) & 31L);
    values[valuesOffset++] = (int) ((block0 >> 4) & 31L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 15L) << 1) | (block1 >> 63));
    values[valuesOffset++] = (int) ((block1 >> 58) & 31L);
    values[valuesOffset++] = (int) ((block1 >> 53) & 31L);
    values[valuesOffset++] = (int) ((block1 >> 48) & 31L);
    values[valuesOffset++] = (int) ((block1 >> 43) & 31L);
    values[valuesOffset++] = (int) ((block1 >> 38) & 31L);
    values[valuesOffset++] = (int) ((block1 >> 33) & 31L);
    values[valuesOffset++] = (int) ((block1 >> 28) & 31L);
    values[valuesOffset++] = (int) ((block1 >> 23) & 31L);
    values[valuesOffset++] = (int) ((block1 >> 18) & 31L);
    values[valuesOffset++] = (int) ((block1 >> 13) & 31L);
    values[valuesOffset++] = (int) ((block1 >> 8) & 31L);
    values[valuesOffset++] = (int) ((block1 >> 3) & 31L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 7L) << 2) | (block2 >> 62));
    values[valuesOffset++] = (int) ((block2 >> 57) & 31L);
    values[valuesOffset++] = (int) ((block2 >> 52) & 31L);
    values[valuesOffset++] = (int) ((block2 >> 47) & 31L);
    values[valuesOffset++] = (int) ((block2 >> 42) & 31L);
    values[valuesOffset++] = (int) ((block2 >> 37) & 31L);
    values[valuesOffset++] = (int) ((block2 >> 32) & 31L);
    values[valuesOffset++] = (int) ((block2 >> 27) & 31L);
    values[valuesOffset++] = (int) ((block2 >> 22) & 31L);
    values[valuesOffset++] = (int) ((block2 >> 17) & 31L);
    values[valuesOffset++] = (int) ((block2 >> 12) & 31L);
    values[valuesOffset++] = (int) ((block2 >> 7) & 31L);
    values[valuesOffset++] = (int) ((block2 >> 2) & 31L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 3L) << 3) | (block3 >> 61));
    values[valuesOffset++] = (int) ((block3 >> 56) & 31L);
    values[valuesOffset++] = (int) ((block3 >> 51) & 31L);
    values[valuesOffset++] = (int) ((block3 >> 46) & 31L);
    values[valuesOffset++] = (int) ((block3 >> 41) & 31L);
    values[valuesOffset++] = (int) ((block3 >> 36) & 31L);
    values[valuesOffset++] = (int) ((block3 >> 31) & 31L);
    values[valuesOffset++] = (int) ((block3 >> 26) & 31L);
    values[valuesOffset++] = (int) ((block3 >> 21) & 31L);
    values[valuesOffset++] = (int) ((block3 >> 16) & 31L);
    values[valuesOffset++] = (int) ((block3 >> 11) & 31L);
    values[valuesOffset++] = (int) ((block3 >> 6) & 31L);
    values[valuesOffset++] = (int) ((block3 >> 1) & 31L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 1L) << 4) | (block4 >> 60));
    values[valuesOffset++] = (int) ((block4 >> 55) & 31L);
    values[valuesOffset++] = (int) ((block4 >> 50) & 31L);
    values[valuesOffset++] = (int) ((block4 >> 45) & 31L);
    values[valuesOffset++] = (int) ((block4 >> 40) & 31L);
    values[valuesOffset++] = (int) ((block4 >> 35) & 31L);
    values[valuesOffset++] = (int) ((block4 >> 30) & 31L);
    values[valuesOffset++] = (int) ((block4 >> 25) & 31L);
    values[valuesOffset++] = (int) ((block4 >> 20) & 31L);
    values[valuesOffset++] = (int) ((block4 >> 15) & 31L);
    values[valuesOffset++] = (int) ((block4 >> 10) & 31L);
    values[valuesOffset++] = (int) ((block4 >> 5) & 31L);
    values[valuesOffset++] = (int) (block4 & 31L);
  }
}

static void decode6(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 4; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 58);
    values[valuesOffset++] = (int) ((block0 >> 52) & 63L);
    values[valuesOffset++] = (int) ((block0 >> 46) & 63L);
    values[valuesOffset++] = (int) ((block0 >> 40) & 63L);
    values[valuesOffset++] = (int) ((block0 >> 34) & 63L);
    values[valuesOffset++] = (int) ((block0 >> 28) & 63L);
    values[valuesOffset++] = (int) ((block0 >> 22) & 63L);
    values[valuesOffset++] = (int) ((block0 >> 16) & 63L);
    values[valuesOffset++] = (int) ((block0 >> 10) & 63L);
    values[valuesOffset++] = (int) ((block0 >> 4) & 63L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 15L) << 2) | (block1 >> 62));
    values[valuesOffset++] = (int) ((block1 >> 56) & 63L);
    values[valuesOffset++] = (int) ((block1 >> 50) & 63L);
    values[valuesOffset++] = (int) ((block1 >> 44) & 63L);
    values[valuesOffset++] = (int) ((block1 >> 38) & 63L);
    values[valuesOffset++] = (int) ((block1 >> 32) & 63L);
    values[valuesOffset++] = (int) ((block1 >> 26) & 63L);
    values[valuesOffset++] = (int) ((block1 >> 20) & 63L);
    values[valuesOffset++] = (int) ((block1 >> 14) & 63L);
    values[valuesOffset++] = (int) ((block1 >> 8) & 63L);
    values[valuesOffset++] = (int) ((block1 >> 2) & 63L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 3L) << 4) | (block2 >> 60));
    values[valuesOffset++] = (int) ((block2 >> 54) & 63L);
    values[valuesOffset++] = (int) ((block2 >> 48) & 63L);
    values[valuesOffset++] = (int) ((block2 >> 42) & 63L);
    values[valuesOffset++] = (int) ((block2 >> 36) & 63L);
    values[valuesOffset++] = (int) ((block2 >> 30) & 63L);
    values[valuesOffset++] = (int) ((block2 >> 24) & 63L);
    values[valuesOffset++] = (int) ((block2 >> 18) & 63L);
    values[valuesOffset++] = (int) ((block2 >> 12) & 63L);
    values[valuesOffset++] = (int) ((block2 >> 6) & 63L);
    values[valuesOffset++] = (int) (block2 & 63L);
  }
}

static void decode7(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 2; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 57);
    values[valuesOffset++] = (int) ((block0 >> 50) & 127L);
    values[valuesOffset++] = (int) ((block0 >> 43) & 127L);
    values[valuesOffset++] = (int) ((block0 >> 36) & 127L);
    values[valuesOffset++] = (int) ((block0 >> 29) & 127L);
    values[valuesOffset++] = (int) ((block0 >> 22) & 127L);
    values[valuesOffset++] = (int) ((block0 >> 15) & 127L);
    values[valuesOffset++] = (int) ((block0 >> 8) & 127L);
    values[valuesOffset++] = (int) ((block0 >> 1) & 127L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 1L) << 6) | (block1 >> 58));
    values[valuesOffset++] = (int) ((block1 >> 51) & 127L);
    values[valuesOffset++] = (int) ((block1 >> 44) & 127L);
    values[valuesOffset++] = (int) ((block1 >> 37) & 127L);
    values[valuesOffset++] = (int) ((block1 >> 30) & 127L);
    values[valuesOffset++] = (int) ((block1 >> 23) & 127L);
    values[valuesOffset++] = (int) ((block1 >> 16) & 127L);
    values[valuesOffset++] = (int) ((block1 >> 9) & 127L);
    values[valuesOffset++] = (int) ((block1 >> 2) & 127L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 3L) << 5) | (block2 >> 59));
    values[valuesOffset++] = (int) ((block2 >> 52) & 127L);
    values[valuesOffset++] = (int) ((block2 >> 45) & 127L);
    values[valuesOffset++] = (int) ((block2 >> 38) & 127L);
    values[valuesOffset++] = (int) ((block2 >> 31) & 127L);
    values[valuesOffset++] = (int) ((block2 >> 24) & 127L);
    values[valuesOffset++] = (int) ((block2 >> 17) & 127L);
    values[valuesOffset++] = (int) ((block2 >> 10) & 127L);
    values[valuesOffset++] = (int) ((block2 >> 3) & 127L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 7L) << 4) | (block3 >> 60));
    values[valuesOffset++] = (int) ((block3 >> 53) & 127L);
    values[valuesOffset++] = (int) ((block3 >> 46) & 127L);
    values[valuesOffset++] = (int) ((block3 >> 39) & 127L);
    values[valuesOffset++] = (int) ((block3 >> 32) & 127L);
    values[valuesOffset++] = (int) ((block3 >> 25) & 127L);
    values[valuesOffset++] = (int) ((block3 >> 18) & 127L);
    values[valuesOffset++] = (int) ((block3 >> 11) & 127L);
    values[valuesOffset++] = (int) ((block3 >> 4) & 127L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 15L) << 3) | (block4 >> 61));
    values[valuesOffset++] = (int) ((block4 >> 54) & 127L);
    values[valuesOffset++] = (int) ((block4 >> 47) & 127L);
    values[valuesOffset++] = (int) ((block4 >> 40) & 127L);
    values[valuesOffset++] = (int) ((block4 >> 33) & 127L);
    values[valuesOffset++] = (int) ((block4 >> 26) & 127L);
    values[valuesOffset++] = (int) ((block4 >> 19) & 127L);
    values[valuesOffset++] = (int) ((block4 >> 12) & 127L);
    values[valuesOffset++] = (int) ((block4 >> 5) & 127L);
    unsigned long block5 = blocks[blocksOffset++];
    block5 = __bswap_64(block5);
    values[valuesOffset++] = (int) (((block4 & 31L) << 2) | (block5 >> 62));
    values[valuesOffset++] = (int) ((block5 >> 55) & 127L);
    values[valuesOffset++] = (int) ((block5 >> 48) & 127L);
    values[valuesOffset++] = (int) ((block5 >> 41) & 127L);
    values[valuesOffset++] = (int) ((block5 >> 34) & 127L);
    values[valuesOffset++] = (int) ((block5 >> 27) & 127L);
    values[valuesOffset++] = (int) ((block5 >> 20) & 127L);
    values[valuesOffset++] = (int) ((block5 >> 13) & 127L);
    values[valuesOffset++] = (int) ((block5 >> 6) & 127L);
    unsigned long block6 = blocks[blocksOffset++];
    block6 = __bswap_64(block6);
    values[valuesOffset++] = (int) (((block5 & 63L) << 1) | (block6 >> 63));
    values[valuesOffset++] = (int) ((block6 >> 56) & 127L);
    values[valuesOffset++] = (int) ((block6 >> 49) & 127L);
    values[valuesOffset++] = (int) ((block6 >> 42) & 127L);
    values[valuesOffset++] = (int) ((block6 >> 35) & 127L);
    values[valuesOffset++] = (int) ((block6 >> 28) & 127L);
    values[valuesOffset++] = (int) ((block6 >> 21) & 127L);
    values[valuesOffset++] = (int) ((block6 >> 14) & 127L);
    values[valuesOffset++] = (int) ((block6 >> 7) & 127L);
    values[valuesOffset++] = (int) (block6 & 127L);
  }
}

static void decode8(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 16; ++i) {
    unsigned long block = blocks[blocksOffset++];
    block = __bswap_64(block);
    for (int shift = 56; shift >= 0; shift -= 8) {
      values[valuesOffset++] = (int) ((block >> shift) & 255);
    }
  }
}

static void decode9(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 2; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 55);
    values[valuesOffset++] = (int) ((block0 >> 46) & 511L);
    values[valuesOffset++] = (int) ((block0 >> 37) & 511L);
    values[valuesOffset++] = (int) ((block0 >> 28) & 511L);
    values[valuesOffset++] = (int) ((block0 >> 19) & 511L);
    values[valuesOffset++] = (int) ((block0 >> 10) & 511L);
    values[valuesOffset++] = (int) ((block0 >> 1) & 511L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 1L) << 8) | (block1 >> 56));
    values[valuesOffset++] = (int) ((block1 >> 47) & 511L);
    values[valuesOffset++] = (int) ((block1 >> 38) & 511L);
    values[valuesOffset++] = (int) ((block1 >> 29) & 511L);
    values[valuesOffset++] = (int) ((block1 >> 20) & 511L);
    values[valuesOffset++] = (int) ((block1 >> 11) & 511L);
    values[valuesOffset++] = (int) ((block1 >> 2) & 511L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 3L) << 7) | (block2 >> 57));
    values[valuesOffset++] = (int) ((block2 >> 48) & 511L);
    values[valuesOffset++] = (int) ((block2 >> 39) & 511L);
    values[valuesOffset++] = (int) ((block2 >> 30) & 511L);
    values[valuesOffset++] = (int) ((block2 >> 21) & 511L);
    values[valuesOffset++] = (int) ((block2 >> 12) & 511L);
    values[valuesOffset++] = (int) ((block2 >> 3) & 511L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 7L) << 6) | (block3 >> 58));
    values[valuesOffset++] = (int) ((block3 >> 49) & 511L);
    values[valuesOffset++] = (int) ((block3 >> 40) & 511L);
    values[valuesOffset++] = (int) ((block3 >> 31) & 511L);
    values[valuesOffset++] = (int) ((block3 >> 22) & 511L);
    values[valuesOffset++] = (int) ((block3 >> 13) & 511L);
    values[valuesOffset++] = (int) ((block3 >> 4) & 511L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 15L) << 5) | (block4 >> 59));
    values[valuesOffset++] = (int) ((block4 >> 50) & 511L);
    values[valuesOffset++] = (int) ((block4 >> 41) & 511L);
    values[valuesOffset++] = (int) ((block4 >> 32) & 511L);
    values[valuesOffset++] = (int) ((block4 >> 23) & 511L);
    values[valuesOffset++] = (int) ((block4 >> 14) & 511L);
    values[valuesOffset++] = (int) ((block4 >> 5) & 511L);
    unsigned long block5 = blocks[blocksOffset++];
    block5 = __bswap_64(block5);
    values[valuesOffset++] = (int) (((block4 & 31L) << 4) | (block5 >> 60));
    values[valuesOffset++] = (int) ((block5 >> 51) & 511L);
    values[valuesOffset++] = (int) ((block5 >> 42) & 511L);
    values[valuesOffset++] = (int) ((block5 >> 33) & 511L);
    values[valuesOffset++] = (int) ((block5 >> 24) & 511L);
    values[valuesOffset++] = (int) ((block5 >> 15) & 511L);
    values[valuesOffset++] = (int) ((block5 >> 6) & 511L);
    unsigned long block6 = blocks[blocksOffset++];
    block6 = __bswap_64(block6);
    values[valuesOffset++] = (int) (((block5 & 63L) << 3) | (block6 >> 61));
    values[valuesOffset++] = (int) ((block6 >> 52) & 511L);
    values[valuesOffset++] = (int) ((block6 >> 43) & 511L);
    values[valuesOffset++] = (int) ((block6 >> 34) & 511L);
    values[valuesOffset++] = (int) ((block6 >> 25) & 511L);
    values[valuesOffset++] = (int) ((block6 >> 16) & 511L);
    values[valuesOffset++] = (int) ((block6 >> 7) & 511L);
    unsigned long block7 = blocks[blocksOffset++];
    block7 = __bswap_64(block7);
    values[valuesOffset++] = (int) (((block6 & 127L) << 2) | (block7 >> 62));
    values[valuesOffset++] = (int) ((block7 >> 53) & 511L);
    values[valuesOffset++] = (int) ((block7 >> 44) & 511L);
    values[valuesOffset++] = (int) ((block7 >> 35) & 511L);
    values[valuesOffset++] = (int) ((block7 >> 26) & 511L);
    values[valuesOffset++] = (int) ((block7 >> 17) & 511L);
    values[valuesOffset++] = (int) ((block7 >> 8) & 511L);
    unsigned long block8 = blocks[blocksOffset++];
    block8 = __bswap_64(block8);
    values[valuesOffset++] = (int) (((block7 & 255L) << 1) | (block8 >> 63));
    values[valuesOffset++] = (int) ((block8 >> 54) & 511L);
    values[valuesOffset++] = (int) ((block8 >> 45) & 511L);
    values[valuesOffset++] = (int) ((block8 >> 36) & 511L);
    values[valuesOffset++] = (int) ((block8 >> 27) & 511L);
    values[valuesOffset++] = (int) ((block8 >> 18) & 511L);
    values[valuesOffset++] = (int) ((block8 >> 9) & 511L);
    values[valuesOffset++] = (int) (block8 & 511L);
  }
}

static void decode10(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 4; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 54);
    values[valuesOffset++] = (int) ((block0 >> 44) & 1023L);
    values[valuesOffset++] = (int) ((block0 >> 34) & 1023L);
    values[valuesOffset++] = (int) ((block0 >> 24) & 1023L);
    values[valuesOffset++] = (int) ((block0 >> 14) & 1023L);
    values[valuesOffset++] = (int) ((block0 >> 4) & 1023L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 15L) << 6) | (block1 >> 58));
    values[valuesOffset++] = (int) ((block1 >> 48) & 1023L);
    values[valuesOffset++] = (int) ((block1 >> 38) & 1023L);
    values[valuesOffset++] = (int) ((block1 >> 28) & 1023L);
    values[valuesOffset++] = (int) ((block1 >> 18) & 1023L);
    values[valuesOffset++] = (int) ((block1 >> 8) & 1023L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 255L) << 2) | (block2 >> 62));
    values[valuesOffset++] = (int) ((block2 >> 52) & 1023L);
    values[valuesOffset++] = (int) ((block2 >> 42) & 1023L);
    values[valuesOffset++] = (int) ((block2 >> 32) & 1023L);
    values[valuesOffset++] = (int) ((block2 >> 22) & 1023L);
    values[valuesOffset++] = (int) ((block2 >> 12) & 1023L);
    values[valuesOffset++] = (int) ((block2 >> 2) & 1023L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 3L) << 8) | (block3 >> 56));
    values[valuesOffset++] = (int) ((block3 >> 46) & 1023L);
    values[valuesOffset++] = (int) ((block3 >> 36) & 1023L);
    values[valuesOffset++] = (int) ((block3 >> 26) & 1023L);
    values[valuesOffset++] = (int) ((block3 >> 16) & 1023L);
    values[valuesOffset++] = (int) ((block3 >> 6) & 1023L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 63L) << 4) | (block4 >> 60));
    values[valuesOffset++] = (int) ((block4 >> 50) & 1023L);
    values[valuesOffset++] = (int) ((block4 >> 40) & 1023L);
    values[valuesOffset++] = (int) ((block4 >> 30) & 1023L);
    values[valuesOffset++] = (int) ((block4 >> 20) & 1023L);
    values[valuesOffset++] = (int) ((block4 >> 10) & 1023L);
    values[valuesOffset++] = (int) (block4 & 1023L);
  }
}

static void decode11(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 2; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 53);
    values[valuesOffset++] = (int) ((block0 >> 42) & 2047L);
    values[valuesOffset++] = (int) ((block0 >> 31) & 2047L);
    values[valuesOffset++] = (int) ((block0 >> 20) & 2047L);
    values[valuesOffset++] = (int) ((block0 >> 9) & 2047L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 511L) << 2) | (block1 >> 62));
    values[valuesOffset++] = (int) ((block1 >> 51) & 2047L);
    values[valuesOffset++] = (int) ((block1 >> 40) & 2047L);
    values[valuesOffset++] = (int) ((block1 >> 29) & 2047L);
    values[valuesOffset++] = (int) ((block1 >> 18) & 2047L);
    values[valuesOffset++] = (int) ((block1 >> 7) & 2047L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 127L) << 4) | (block2 >> 60));
    values[valuesOffset++] = (int) ((block2 >> 49) & 2047L);
    values[valuesOffset++] = (int) ((block2 >> 38) & 2047L);
    values[valuesOffset++] = (int) ((block2 >> 27) & 2047L);
    values[valuesOffset++] = (int) ((block2 >> 16) & 2047L);
    values[valuesOffset++] = (int) ((block2 >> 5) & 2047L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 31L) << 6) | (block3 >> 58));
    values[valuesOffset++] = (int) ((block3 >> 47) & 2047L);
    values[valuesOffset++] = (int) ((block3 >> 36) & 2047L);
    values[valuesOffset++] = (int) ((block3 >> 25) & 2047L);
    values[valuesOffset++] = (int) ((block3 >> 14) & 2047L);
    values[valuesOffset++] = (int) ((block3 >> 3) & 2047L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 7L) << 8) | (block4 >> 56));
    values[valuesOffset++] = (int) ((block4 >> 45) & 2047L);
    values[valuesOffset++] = (int) ((block4 >> 34) & 2047L);
    values[valuesOffset++] = (int) ((block4 >> 23) & 2047L);
    values[valuesOffset++] = (int) ((block4 >> 12) & 2047L);
    values[valuesOffset++] = (int) ((block4 >> 1) & 2047L);
    unsigned long block5 = blocks[blocksOffset++];
    block5 = __bswap_64(block5);
    values[valuesOffset++] = (int) (((block4 & 1L) << 10) | (block5 >> 54));
    values[valuesOffset++] = (int) ((block5 >> 43) & 2047L);
    values[valuesOffset++] = (int) ((block5 >> 32) & 2047L);
    values[valuesOffset++] = (int) ((block5 >> 21) & 2047L);
    values[valuesOffset++] = (int) ((block5 >> 10) & 2047L);
    unsigned long block6 = blocks[blocksOffset++];
    block6 = __bswap_64(block6);
    values[valuesOffset++] = (int) (((block5 & 1023L) << 1) | (block6 >> 63));
    values[valuesOffset++] = (int) ((block6 >> 52) & 2047L);
    values[valuesOffset++] = (int) ((block6 >> 41) & 2047L);
    values[valuesOffset++] = (int) ((block6 >> 30) & 2047L);
    values[valuesOffset++] = (int) ((block6 >> 19) & 2047L);
    values[valuesOffset++] = (int) ((block6 >> 8) & 2047L);
    unsigned long block7 = blocks[blocksOffset++];
    block7 = __bswap_64(block7);
    values[valuesOffset++] = (int) (((block6 & 255L) << 3) | (block7 >> 61));
    values[valuesOffset++] = (int) ((block7 >> 50) & 2047L);
    values[valuesOffset++] = (int) ((block7 >> 39) & 2047L);
    values[valuesOffset++] = (int) ((block7 >> 28) & 2047L);
    values[valuesOffset++] = (int) ((block7 >> 17) & 2047L);
    values[valuesOffset++] = (int) ((block7 >> 6) & 2047L);
    unsigned long block8 = blocks[blocksOffset++];
    block8 = __bswap_64(block8);
    values[valuesOffset++] = (int) (((block7 & 63L) << 5) | (block8 >> 59));
    values[valuesOffset++] = (int) ((block8 >> 48) & 2047L);
    values[valuesOffset++] = (int) ((block8 >> 37) & 2047L);
    values[valuesOffset++] = (int) ((block8 >> 26) & 2047L);
    values[valuesOffset++] = (int) ((block8 >> 15) & 2047L);
    values[valuesOffset++] = (int) ((block8 >> 4) & 2047L);
    unsigned long block9 = blocks[blocksOffset++];
    block9 = __bswap_64(block9);
    values[valuesOffset++] = (int) (((block8 & 15L) << 7) | (block9 >> 57));
    values[valuesOffset++] = (int) ((block9 >> 46) & 2047L);
    values[valuesOffset++] = (int) ((block9 >> 35) & 2047L);
    values[valuesOffset++] = (int) ((block9 >> 24) & 2047L);
    values[valuesOffset++] = (int) ((block9 >> 13) & 2047L);
    values[valuesOffset++] = (int) ((block9 >> 2) & 2047L);
    unsigned long block10 = blocks[blocksOffset++];
    block10 = __bswap_64(block10);
    values[valuesOffset++] = (int) (((block9 & 3L) << 9) | (block10 >> 55));
    values[valuesOffset++] = (int) ((block10 >> 44) & 2047L);
    values[valuesOffset++] = (int) ((block10 >> 33) & 2047L);
    values[valuesOffset++] = (int) ((block10 >> 22) & 2047L);
    values[valuesOffset++] = (int) ((block10 >> 11) & 2047L);
    values[valuesOffset++] = (int) (block10 & 2047L);
  }
}

static void decode12(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 8; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 52);
    values[valuesOffset++] = (int) ((block0 >> 40) & 4095L);
    values[valuesOffset++] = (int) ((block0 >> 28) & 4095L);
    values[valuesOffset++] = (int) ((block0 >> 16) & 4095L);
    values[valuesOffset++] = (int) ((block0 >> 4) & 4095L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 15L) << 8) | (block1 >> 56));
    values[valuesOffset++] = (int) ((block1 >> 44) & 4095L);
    values[valuesOffset++] = (int) ((block1 >> 32) & 4095L);
    values[valuesOffset++] = (int) ((block1 >> 20) & 4095L);
    values[valuesOffset++] = (int) ((block1 >> 8) & 4095L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 255L) << 4) | (block2 >> 60));
    values[valuesOffset++] = (int) ((block2 >> 48) & 4095L);
    values[valuesOffset++] = (int) ((block2 >> 36) & 4095L);
    values[valuesOffset++] = (int) ((block2 >> 24) & 4095L);
    values[valuesOffset++] = (int) ((block2 >> 12) & 4095L);
    values[valuesOffset++] = (int) (block2 & 4095L);
  }
}

static void decode13(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 2; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 51);
    values[valuesOffset++] = (int) ((block0 >> 38) & 8191L);
    values[valuesOffset++] = (int) ((block0 >> 25) & 8191L);
    values[valuesOffset++] = (int) ((block0 >> 12) & 8191L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 4095L) << 1) | (block1 >> 63));
    values[valuesOffset++] = (int) ((block1 >> 50) & 8191L);
    values[valuesOffset++] = (int) ((block1 >> 37) & 8191L);
    values[valuesOffset++] = (int) ((block1 >> 24) & 8191L);
    values[valuesOffset++] = (int) ((block1 >> 11) & 8191L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 2047L) << 2) | (block2 >> 62));
    values[valuesOffset++] = (int) ((block2 >> 49) & 8191L);
    values[valuesOffset++] = (int) ((block2 >> 36) & 8191L);
    values[valuesOffset++] = (int) ((block2 >> 23) & 8191L);
    values[valuesOffset++] = (int) ((block2 >> 10) & 8191L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 1023L) << 3) | (block3 >> 61));
    values[valuesOffset++] = (int) ((block3 >> 48) & 8191L);
    values[valuesOffset++] = (int) ((block3 >> 35) & 8191L);
    values[valuesOffset++] = (int) ((block3 >> 22) & 8191L);
    values[valuesOffset++] = (int) ((block3 >> 9) & 8191L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 511L) << 4) | (block4 >> 60));
    values[valuesOffset++] = (int) ((block4 >> 47) & 8191L);
    values[valuesOffset++] = (int) ((block4 >> 34) & 8191L);
    values[valuesOffset++] = (int) ((block4 >> 21) & 8191L);
    values[valuesOffset++] = (int) ((block4 >> 8) & 8191L);
    unsigned long block5 = blocks[blocksOffset++];
    block5 = __bswap_64(block5);
    values[valuesOffset++] = (int) (((block4 & 255L) << 5) | (block5 >> 59));
    values[valuesOffset++] = (int) ((block5 >> 46) & 8191L);
    values[valuesOffset++] = (int) ((block5 >> 33) & 8191L);
    values[valuesOffset++] = (int) ((block5 >> 20) & 8191L);
    values[valuesOffset++] = (int) ((block5 >> 7) & 8191L);
    unsigned long block6 = blocks[blocksOffset++];
    block6 = __bswap_64(block6);
    values[valuesOffset++] = (int) (((block5 & 127L) << 6) | (block6 >> 58));
    values[valuesOffset++] = (int) ((block6 >> 45) & 8191L);
    values[valuesOffset++] = (int) ((block6 >> 32) & 8191L);
    values[valuesOffset++] = (int) ((block6 >> 19) & 8191L);
    values[valuesOffset++] = (int) ((block6 >> 6) & 8191L);
    unsigned long block7 = blocks[blocksOffset++];
    block7 = __bswap_64(block7);
    values[valuesOffset++] = (int) (((block6 & 63L) << 7) | (block7 >> 57));
    values[valuesOffset++] = (int) ((block7 >> 44) & 8191L);
    values[valuesOffset++] = (int) ((block7 >> 31) & 8191L);
    values[valuesOffset++] = (int) ((block7 >> 18) & 8191L);
    values[valuesOffset++] = (int) ((block7 >> 5) & 8191L);
    unsigned long block8 = blocks[blocksOffset++];
    block8 = __bswap_64(block8);
    values[valuesOffset++] = (int) (((block7 & 31L) << 8) | (block8 >> 56));
    values[valuesOffset++] = (int) ((block8 >> 43) & 8191L);
    values[valuesOffset++] = (int) ((block8 >> 30) & 8191L);
    values[valuesOffset++] = (int) ((block8 >> 17) & 8191L);
    values[valuesOffset++] = (int) ((block8 >> 4) & 8191L);
    unsigned long block9 = blocks[blocksOffset++];
    block9 = __bswap_64(block9);
    values[valuesOffset++] = (int) (((block8 & 15L) << 9) | (block9 >> 55));
    values[valuesOffset++] = (int) ((block9 >> 42) & 8191L);
    values[valuesOffset++] = (int) ((block9 >> 29) & 8191L);
    values[valuesOffset++] = (int) ((block9 >> 16) & 8191L);
    values[valuesOffset++] = (int) ((block9 >> 3) & 8191L);
    unsigned long block10 = blocks[blocksOffset++];
    block10 = __bswap_64(block10);
    values[valuesOffset++] = (int) (((block9 & 7L) << 10) | (block10 >> 54));
    values[valuesOffset++] = (int) ((block10 >> 41) & 8191L);
    values[valuesOffset++] = (int) ((block10 >> 28) & 8191L);
    values[valuesOffset++] = (int) ((block10 >> 15) & 8191L);
    values[valuesOffset++] = (int) ((block10 >> 2) & 8191L);
    unsigned long block11 = blocks[blocksOffset++];
    block11 = __bswap_64(block11);
    values[valuesOffset++] = (int) (((block10 & 3L) << 11) | (block11 >> 53));
    values[valuesOffset++] = (int) ((block11 >> 40) & 8191L);
    values[valuesOffset++] = (int) ((block11 >> 27) & 8191L);
    values[valuesOffset++] = (int) ((block11 >> 14) & 8191L);
    values[valuesOffset++] = (int) ((block11 >> 1) & 8191L);
    unsigned long block12 = blocks[blocksOffset++];
    block12 = __bswap_64(block12);
    values[valuesOffset++] = (int) (((block11 & 1L) << 12) | (block12 >> 52));
    values[valuesOffset++] = (int) ((block12 >> 39) & 8191L);
    values[valuesOffset++] = (int) ((block12 >> 26) & 8191L);
    values[valuesOffset++] = (int) ((block12 >> 13) & 8191L);
    values[valuesOffset++] = (int) (block12 & 8191L);
  }
}

static void decode14(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 4; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 50);
    values[valuesOffset++] = (int) ((block0 >> 36) & 16383L);
    values[valuesOffset++] = (int) ((block0 >> 22) & 16383L);
    values[valuesOffset++] = (int) ((block0 >> 8) & 16383L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 255L) << 6) | (block1 >> 58));
    values[valuesOffset++] = (int) ((block1 >> 44) & 16383L);
    values[valuesOffset++] = (int) ((block1 >> 30) & 16383L);
    values[valuesOffset++] = (int) ((block1 >> 16) & 16383L);
    values[valuesOffset++] = (int) ((block1 >> 2) & 16383L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 3L) << 12) | (block2 >> 52));
    values[valuesOffset++] = (int) ((block2 >> 38) & 16383L);
    values[valuesOffset++] = (int) ((block2 >> 24) & 16383L);
    values[valuesOffset++] = (int) ((block2 >> 10) & 16383L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 1023L) << 4) | (block3 >> 60));
    values[valuesOffset++] = (int) ((block3 >> 46) & 16383L);
    values[valuesOffset++] = (int) ((block3 >> 32) & 16383L);
    values[valuesOffset++] = (int) ((block3 >> 18) & 16383L);
    values[valuesOffset++] = (int) ((block3 >> 4) & 16383L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 15L) << 10) | (block4 >> 54));
    values[valuesOffset++] = (int) ((block4 >> 40) & 16383L);
    values[valuesOffset++] = (int) ((block4 >> 26) & 16383L);
    values[valuesOffset++] = (int) ((block4 >> 12) & 16383L);
    unsigned long block5 = blocks[blocksOffset++];
    block5 = __bswap_64(block5);
    values[valuesOffset++] = (int) (((block4 & 4095L) << 2) | (block5 >> 62));
    values[valuesOffset++] = (int) ((block5 >> 48) & 16383L);
    values[valuesOffset++] = (int) ((block5 >> 34) & 16383L);
    values[valuesOffset++] = (int) ((block5 >> 20) & 16383L);
    values[valuesOffset++] = (int) ((block5 >> 6) & 16383L);
    unsigned long block6 = blocks[blocksOffset++];
    block6 = __bswap_64(block6);
    values[valuesOffset++] = (int) (((block5 & 63L) << 8) | (block6 >> 56));
    values[valuesOffset++] = (int) ((block6 >> 42) & 16383L);
    values[valuesOffset++] = (int) ((block6 >> 28) & 16383L);
    values[valuesOffset++] = (int) ((block6 >> 14) & 16383L);
    values[valuesOffset++] = (int) (block6 & 16383L);
  }
}

static void decode15(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 2; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 49);
    values[valuesOffset++] = (int) ((block0 >> 34) & 32767L);
    values[valuesOffset++] = (int) ((block0 >> 19) & 32767L);
    values[valuesOffset++] = (int) ((block0 >> 4) & 32767L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 15L) << 11) | (block1 >> 53));
    values[valuesOffset++] = (int) ((block1 >> 38) & 32767L);
    values[valuesOffset++] = (int) ((block1 >> 23) & 32767L);
    values[valuesOffset++] = (int) ((block1 >> 8) & 32767L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 255L) << 7) | (block2 >> 57));
    values[valuesOffset++] = (int) ((block2 >> 42) & 32767L);
    values[valuesOffset++] = (int) ((block2 >> 27) & 32767L);
    values[valuesOffset++] = (int) ((block2 >> 12) & 32767L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 4095L) << 3) | (block3 >> 61));
    values[valuesOffset++] = (int) ((block3 >> 46) & 32767L);
    values[valuesOffset++] = (int) ((block3 >> 31) & 32767L);
    values[valuesOffset++] = (int) ((block3 >> 16) & 32767L);
    values[valuesOffset++] = (int) ((block3 >> 1) & 32767L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 1L) << 14) | (block4 >> 50));
    values[valuesOffset++] = (int) ((block4 >> 35) & 32767L);
    values[valuesOffset++] = (int) ((block4 >> 20) & 32767L);
    values[valuesOffset++] = (int) ((block4 >> 5) & 32767L);
    unsigned long block5 = blocks[blocksOffset++];
    block5 = __bswap_64(block5);
    values[valuesOffset++] = (int) (((block4 & 31L) << 10) | (block5 >> 54));
    values[valuesOffset++] = (int) ((block5 >> 39) & 32767L);
    values[valuesOffset++] = (int) ((block5 >> 24) & 32767L);
    values[valuesOffset++] = (int) ((block5 >> 9) & 32767L);
    unsigned long block6 = blocks[blocksOffset++];
    block6 = __bswap_64(block6);
    values[valuesOffset++] = (int) (((block5 & 511L) << 6) | (block6 >> 58));
    values[valuesOffset++] = (int) ((block6 >> 43) & 32767L);
    values[valuesOffset++] = (int) ((block6 >> 28) & 32767L);
    values[valuesOffset++] = (int) ((block6 >> 13) & 32767L);
    unsigned long block7 = blocks[blocksOffset++];
    block7 = __bswap_64(block7);
    values[valuesOffset++] = (int) (((block6 & 8191L) << 2) | (block7 >> 62));
    values[valuesOffset++] = (int) ((block7 >> 47) & 32767L);
    values[valuesOffset++] = (int) ((block7 >> 32) & 32767L);
    values[valuesOffset++] = (int) ((block7 >> 17) & 32767L);
    values[valuesOffset++] = (int) ((block7 >> 2) & 32767L);
    unsigned long block8 = blocks[blocksOffset++];
    block8 = __bswap_64(block8);
    values[valuesOffset++] = (int) (((block7 & 3L) << 13) | (block8 >> 51));
    values[valuesOffset++] = (int) ((block8 >> 36) & 32767L);
    values[valuesOffset++] = (int) ((block8 >> 21) & 32767L);
    values[valuesOffset++] = (int) ((block8 >> 6) & 32767L);
    unsigned long block9 = blocks[blocksOffset++];
    block9 = __bswap_64(block9);
    values[valuesOffset++] = (int) (((block8 & 63L) << 9) | (block9 >> 55));
    values[valuesOffset++] = (int) ((block9 >> 40) & 32767L);
    values[valuesOffset++] = (int) ((block9 >> 25) & 32767L);
    values[valuesOffset++] = (int) ((block9 >> 10) & 32767L);
    unsigned long block10 = blocks[blocksOffset++];
    block10 = __bswap_64(block10);
    values[valuesOffset++] = (int) (((block9 & 1023L) << 5) | (block10 >> 59));
    values[valuesOffset++] = (int) ((block10 >> 44) & 32767L);
    values[valuesOffset++] = (int) ((block10 >> 29) & 32767L);
    values[valuesOffset++] = (int) ((block10 >> 14) & 32767L);
    unsigned long block11 = blocks[blocksOffset++];
    block11 = __bswap_64(block11);
    values[valuesOffset++] = (int) (((block10 & 16383L) << 1) | (block11 >> 63));
    values[valuesOffset++] = (int) ((block11 >> 48) & 32767L);
    values[valuesOffset++] = (int) ((block11 >> 33) & 32767L);
    values[valuesOffset++] = (int) ((block11 >> 18) & 32767L);
    values[valuesOffset++] = (int) ((block11 >> 3) & 32767L);
    unsigned long block12 = blocks[blocksOffset++];
    block12 = __bswap_64(block12);
    values[valuesOffset++] = (int) (((block11 & 7L) << 12) | (block12 >> 52));
    values[valuesOffset++] = (int) ((block12 >> 37) & 32767L);
    values[valuesOffset++] = (int) ((block12 >> 22) & 32767L);
    values[valuesOffset++] = (int) ((block12 >> 7) & 32767L);
    unsigned long block13 = blocks[blocksOffset++];
    block13 = __bswap_64(block13);
    values[valuesOffset++] = (int) (((block12 & 127L) << 8) | (block13 >> 56));
    values[valuesOffset++] = (int) ((block13 >> 41) & 32767L);
    values[valuesOffset++] = (int) ((block13 >> 26) & 32767L);
    values[valuesOffset++] = (int) ((block13 >> 11) & 32767L);
    unsigned long block14 = blocks[blocksOffset++];
    block14 = __bswap_64(block14);
    values[valuesOffset++] = (int) (((block13 & 2047L) << 4) | (block14 >> 60));
    values[valuesOffset++] = (int) ((block14 >> 45) & 32767L);
    values[valuesOffset++] = (int) ((block14 >> 30) & 32767L);
    values[valuesOffset++] = (int) ((block14 >> 15) & 32767L);
    values[valuesOffset++] = (int) (block14 & 32767L);
  }
}

static void decode16(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 32; ++i) {
    unsigned long block = blocks[blocksOffset++];
    block = __bswap_64(block);
    for (int shift = 48; shift >= 0; shift -= 16) {
      values[valuesOffset++] = (int) ((block >> shift) & 65535);
    }
  }
}

static void decode17(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 2; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 47);
    values[valuesOffset++] = (int) ((block0 >> 30) & 131071L);
    values[valuesOffset++] = (int) ((block0 >> 13) & 131071L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 8191L) << 4) | (block1 >> 60));
    values[valuesOffset++] = (int) ((block1 >> 43) & 131071L);
    values[valuesOffset++] = (int) ((block1 >> 26) & 131071L);
    values[valuesOffset++] = (int) ((block1 >> 9) & 131071L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 511L) << 8) | (block2 >> 56));
    values[valuesOffset++] = (int) ((block2 >> 39) & 131071L);
    values[valuesOffset++] = (int) ((block2 >> 22) & 131071L);
    values[valuesOffset++] = (int) ((block2 >> 5) & 131071L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 31L) << 12) | (block3 >> 52));
    values[valuesOffset++] = (int) ((block3 >> 35) & 131071L);
    values[valuesOffset++] = (int) ((block3 >> 18) & 131071L);
    values[valuesOffset++] = (int) ((block3 >> 1) & 131071L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 1L) << 16) | (block4 >> 48));
    values[valuesOffset++] = (int) ((block4 >> 31) & 131071L);
    values[valuesOffset++] = (int) ((block4 >> 14) & 131071L);
    unsigned long block5 = blocks[blocksOffset++];
    block5 = __bswap_64(block5);
    values[valuesOffset++] = (int) (((block4 & 16383L) << 3) | (block5 >> 61));
    values[valuesOffset++] = (int) ((block5 >> 44) & 131071L);
    values[valuesOffset++] = (int) ((block5 >> 27) & 131071L);
    values[valuesOffset++] = (int) ((block5 >> 10) & 131071L);
    unsigned long block6 = blocks[blocksOffset++];
    block6 = __bswap_64(block6);
    values[valuesOffset++] = (int) (((block5 & 1023L) << 7) | (block6 >> 57));
    values[valuesOffset++] = (int) ((block6 >> 40) & 131071L);
    values[valuesOffset++] = (int) ((block6 >> 23) & 131071L);
    values[valuesOffset++] = (int) ((block6 >> 6) & 131071L);
    unsigned long block7 = blocks[blocksOffset++];
    block7 = __bswap_64(block7);
    values[valuesOffset++] = (int) (((block6 & 63L) << 11) | (block7 >> 53));
    values[valuesOffset++] = (int) ((block7 >> 36) & 131071L);
    values[valuesOffset++] = (int) ((block7 >> 19) & 131071L);
    values[valuesOffset++] = (int) ((block7 >> 2) & 131071L);
    unsigned long block8 = blocks[blocksOffset++];
    block8 = __bswap_64(block8);
    values[valuesOffset++] = (int) (((block7 & 3L) << 15) | (block8 >> 49));
    values[valuesOffset++] = (int) ((block8 >> 32) & 131071L);
    values[valuesOffset++] = (int) ((block8 >> 15) & 131071L);
    unsigned long block9 = blocks[blocksOffset++];
    block9 = __bswap_64(block9);
    values[valuesOffset++] = (int) (((block8 & 32767L) << 2) | (block9 >> 62));
    values[valuesOffset++] = (int) ((block9 >> 45) & 131071L);
    values[valuesOffset++] = (int) ((block9 >> 28) & 131071L);
    values[valuesOffset++] = (int) ((block9 >> 11) & 131071L);
    unsigned long block10 = blocks[blocksOffset++];
    block10 = __bswap_64(block10);
    values[valuesOffset++] = (int) (((block9 & 2047L) << 6) | (block10 >> 58));
    values[valuesOffset++] = (int) ((block10 >> 41) & 131071L);
    values[valuesOffset++] = (int) ((block10 >> 24) & 131071L);
    values[valuesOffset++] = (int) ((block10 >> 7) & 131071L);
    unsigned long block11 = blocks[blocksOffset++];
    block11 = __bswap_64(block11);
    values[valuesOffset++] = (int) (((block10 & 127L) << 10) | (block11 >> 54));
    values[valuesOffset++] = (int) ((block11 >> 37) & 131071L);
    values[valuesOffset++] = (int) ((block11 >> 20) & 131071L);
    values[valuesOffset++] = (int) ((block11 >> 3) & 131071L);
    unsigned long block12 = blocks[blocksOffset++];
    block12 = __bswap_64(block12);
    values[valuesOffset++] = (int) (((block11 & 7L) << 14) | (block12 >> 50));
    values[valuesOffset++] = (int) ((block12 >> 33) & 131071L);
    values[valuesOffset++] = (int) ((block12 >> 16) & 131071L);
    unsigned long block13 = blocks[blocksOffset++];
    block13 = __bswap_64(block13);
    values[valuesOffset++] = (int) (((block12 & 65535L) << 1) | (block13 >> 63));
    values[valuesOffset++] = (int) ((block13 >> 46) & 131071L);
    values[valuesOffset++] = (int) ((block13 >> 29) & 131071L);
    values[valuesOffset++] = (int) ((block13 >> 12) & 131071L);
    unsigned long block14 = blocks[blocksOffset++];
    block14 = __bswap_64(block14);
    values[valuesOffset++] = (int) (((block13 & 4095L) << 5) | (block14 >> 59));
    values[valuesOffset++] = (int) ((block14 >> 42) & 131071L);
    values[valuesOffset++] = (int) ((block14 >> 25) & 131071L);
    values[valuesOffset++] = (int) ((block14 >> 8) & 131071L);
    unsigned long block15 = blocks[blocksOffset++];
    block15 = __bswap_64(block15);
    values[valuesOffset++] = (int) (((block14 & 255L) << 9) | (block15 >> 55));
    values[valuesOffset++] = (int) ((block15 >> 38) & 131071L);
    values[valuesOffset++] = (int) ((block15 >> 21) & 131071L);
    values[valuesOffset++] = (int) ((block15 >> 4) & 131071L);
    unsigned long block16 = blocks[blocksOffset++];
    block16 = __bswap_64(block16);
    values[valuesOffset++] = (int) (((block15 & 15L) << 13) | (block16 >> 51));
    values[valuesOffset++] = (int) ((block16 >> 34) & 131071L);
    values[valuesOffset++] = (int) ((block16 >> 17) & 131071L);
    values[valuesOffset++] = (int) (block16 & 131071L);
  }
}

static void decode18(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 4; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 46);
    values[valuesOffset++] = (int) ((block0 >> 28) & 262143L);
    values[valuesOffset++] = (int) ((block0 >> 10) & 262143L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 1023L) << 8) | (block1 >> 56));
    values[valuesOffset++] = (int) ((block1 >> 38) & 262143L);
    values[valuesOffset++] = (int) ((block1 >> 20) & 262143L);
    values[valuesOffset++] = (int) ((block1 >> 2) & 262143L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 3L) << 16) | (block2 >> 48));
    values[valuesOffset++] = (int) ((block2 >> 30) & 262143L);
    values[valuesOffset++] = (int) ((block2 >> 12) & 262143L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 4095L) << 6) | (block3 >> 58));
    values[valuesOffset++] = (int) ((block3 >> 40) & 262143L);
    values[valuesOffset++] = (int) ((block3 >> 22) & 262143L);
    values[valuesOffset++] = (int) ((block3 >> 4) & 262143L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 15L) << 14) | (block4 >> 50));
    values[valuesOffset++] = (int) ((block4 >> 32) & 262143L);
    values[valuesOffset++] = (int) ((block4 >> 14) & 262143L);
    unsigned long block5 = blocks[blocksOffset++];
    block5 = __bswap_64(block5);
    values[valuesOffset++] = (int) (((block4 & 16383L) << 4) | (block5 >> 60));
    values[valuesOffset++] = (int) ((block5 >> 42) & 262143L);
    values[valuesOffset++] = (int) ((block5 >> 24) & 262143L);
    values[valuesOffset++] = (int) ((block5 >> 6) & 262143L);
    unsigned long block6 = blocks[blocksOffset++];
    block6 = __bswap_64(block6);
    values[valuesOffset++] = (int) (((block5 & 63L) << 12) | (block6 >> 52));
    values[valuesOffset++] = (int) ((block6 >> 34) & 262143L);
    values[valuesOffset++] = (int) ((block6 >> 16) & 262143L);
    unsigned long block7 = blocks[blocksOffset++];
    block7 = __bswap_64(block7);
    values[valuesOffset++] = (int) (((block6 & 65535L) << 2) | (block7 >> 62));
    values[valuesOffset++] = (int) ((block7 >> 44) & 262143L);
    values[valuesOffset++] = (int) ((block7 >> 26) & 262143L);
    values[valuesOffset++] = (int) ((block7 >> 8) & 262143L);
    unsigned long block8 = blocks[blocksOffset++];
    block8 = __bswap_64(block8);
    values[valuesOffset++] = (int) (((block7 & 255L) << 10) | (block8 >> 54));
    values[valuesOffset++] = (int) ((block8 >> 36) & 262143L);
    values[valuesOffset++] = (int) ((block8 >> 18) & 262143L);
    values[valuesOffset++] = (int) (block8 & 262143L);
  }
}

static void decode19(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 2; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 45);
    values[valuesOffset++] = (int) ((block0 >> 26) & 524287L);
    values[valuesOffset++] = (int) ((block0 >> 7) & 524287L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 127L) << 12) | (block1 >> 52));
    values[valuesOffset++] = (int) ((block1 >> 33) & 524287L);
    values[valuesOffset++] = (int) ((block1 >> 14) & 524287L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 16383L) << 5) | (block2 >> 59));
    values[valuesOffset++] = (int) ((block2 >> 40) & 524287L);
    values[valuesOffset++] = (int) ((block2 >> 21) & 524287L);
    values[valuesOffset++] = (int) ((block2 >> 2) & 524287L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 3L) << 17) | (block3 >> 47));
    values[valuesOffset++] = (int) ((block3 >> 28) & 524287L);
    values[valuesOffset++] = (int) ((block3 >> 9) & 524287L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 511L) << 10) | (block4 >> 54));
    values[valuesOffset++] = (int) ((block4 >> 35) & 524287L);
    values[valuesOffset++] = (int) ((block4 >> 16) & 524287L);
    unsigned long block5 = blocks[blocksOffset++];
    block5 = __bswap_64(block5);
    values[valuesOffset++] = (int) (((block4 & 65535L) << 3) | (block5 >> 61));
    values[valuesOffset++] = (int) ((block5 >> 42) & 524287L);
    values[valuesOffset++] = (int) ((block5 >> 23) & 524287L);
    values[valuesOffset++] = (int) ((block5 >> 4) & 524287L);
    unsigned long block6 = blocks[blocksOffset++];
    block6 = __bswap_64(block6);
    values[valuesOffset++] = (int) (((block5 & 15L) << 15) | (block6 >> 49));
    values[valuesOffset++] = (int) ((block6 >> 30) & 524287L);
    values[valuesOffset++] = (int) ((block6 >> 11) & 524287L);
    unsigned long block7 = blocks[blocksOffset++];
    block7 = __bswap_64(block7);
    values[valuesOffset++] = (int) (((block6 & 2047L) << 8) | (block7 >> 56));
    values[valuesOffset++] = (int) ((block7 >> 37) & 524287L);
    values[valuesOffset++] = (int) ((block7 >> 18) & 524287L);
    unsigned long block8 = blocks[blocksOffset++];
    block8 = __bswap_64(block8);
    values[valuesOffset++] = (int) (((block7 & 262143L) << 1) | (block8 >> 63));
    values[valuesOffset++] = (int) ((block8 >> 44) & 524287L);
    values[valuesOffset++] = (int) ((block8 >> 25) & 524287L);
    values[valuesOffset++] = (int) ((block8 >> 6) & 524287L);
    unsigned long block9 = blocks[blocksOffset++];
    block9 = __bswap_64(block9);
    values[valuesOffset++] = (int) (((block8 & 63L) << 13) | (block9 >> 51));
    values[valuesOffset++] = (int) ((block9 >> 32) & 524287L);
    values[valuesOffset++] = (int) ((block9 >> 13) & 524287L);
    unsigned long block10 = blocks[blocksOffset++];
    block10 = __bswap_64(block10);
    values[valuesOffset++] = (int) (((block9 & 8191L) << 6) | (block10 >> 58));
    values[valuesOffset++] = (int) ((block10 >> 39) & 524287L);
    values[valuesOffset++] = (int) ((block10 >> 20) & 524287L);
    values[valuesOffset++] = (int) ((block10 >> 1) & 524287L);
    unsigned long block11 = blocks[blocksOffset++];
    block11 = __bswap_64(block11);
    values[valuesOffset++] = (int) (((block10 & 1L) << 18) | (block11 >> 46));
    values[valuesOffset++] = (int) ((block11 >> 27) & 524287L);
    values[valuesOffset++] = (int) ((block11 >> 8) & 524287L);
    unsigned long block12 = blocks[blocksOffset++];
    block12 = __bswap_64(block12);
    values[valuesOffset++] = (int) (((block11 & 255L) << 11) | (block12 >> 53));
    values[valuesOffset++] = (int) ((block12 >> 34) & 524287L);
    values[valuesOffset++] = (int) ((block12 >> 15) & 524287L);
    unsigned long block13 = blocks[blocksOffset++];
    block13 = __bswap_64(block13);
    values[valuesOffset++] = (int) (((block12 & 32767L) << 4) | (block13 >> 60));
    values[valuesOffset++] = (int) ((block13 >> 41) & 524287L);
    values[valuesOffset++] = (int) ((block13 >> 22) & 524287L);
    values[valuesOffset++] = (int) ((block13 >> 3) & 524287L);
    unsigned long block14 = blocks[blocksOffset++];
    block14 = __bswap_64(block14);
    values[valuesOffset++] = (int) (((block13 & 7L) << 16) | (block14 >> 48));
    values[valuesOffset++] = (int) ((block14 >> 29) & 524287L);
    values[valuesOffset++] = (int) ((block14 >> 10) & 524287L);
    unsigned long block15 = blocks[blocksOffset++];
    block15 = __bswap_64(block15);
    values[valuesOffset++] = (int) (((block14 & 1023L) << 9) | (block15 >> 55));
    values[valuesOffset++] = (int) ((block15 >> 36) & 524287L);
    values[valuesOffset++] = (int) ((block15 >> 17) & 524287L);
    unsigned long block16 = blocks[blocksOffset++];
    block16 = __bswap_64(block16);
    values[valuesOffset++] = (int) (((block15 & 131071L) << 2) | (block16 >> 62));
    values[valuesOffset++] = (int) ((block16 >> 43) & 524287L);
    values[valuesOffset++] = (int) ((block16 >> 24) & 524287L);
    values[valuesOffset++] = (int) ((block16 >> 5) & 524287L);
    unsigned long block17 = blocks[blocksOffset++];
    block17 = __bswap_64(block17);
    values[valuesOffset++] = (int) (((block16 & 31L) << 14) | (block17 >> 50));
    values[valuesOffset++] = (int) ((block17 >> 31) & 524287L);
    values[valuesOffset++] = (int) ((block17 >> 12) & 524287L);
    unsigned long block18 = blocks[blocksOffset++];
    block18 = __bswap_64(block18);
    values[valuesOffset++] = (int) (((block17 & 4095L) << 7) | (block18 >> 57));
    values[valuesOffset++] = (int) ((block18 >> 38) & 524287L);
    values[valuesOffset++] = (int) ((block18 >> 19) & 524287L);
    values[valuesOffset++] = (int) (block18 & 524287L);
  }
}

static void decode20(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 8; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 44);
    values[valuesOffset++] = (int) ((block0 >> 24) & 1048575L);
    values[valuesOffset++] = (int) ((block0 >> 4) & 1048575L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 15L) << 16) | (block1 >> 48));
    values[valuesOffset++] = (int) ((block1 >> 28) & 1048575L);
    values[valuesOffset++] = (int) ((block1 >> 8) & 1048575L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 255L) << 12) | (block2 >> 52));
    values[valuesOffset++] = (int) ((block2 >> 32) & 1048575L);
    values[valuesOffset++] = (int) ((block2 >> 12) & 1048575L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 4095L) << 8) | (block3 >> 56));
    values[valuesOffset++] = (int) ((block3 >> 36) & 1048575L);
    values[valuesOffset++] = (int) ((block3 >> 16) & 1048575L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 65535L) << 4) | (block4 >> 60));
    values[valuesOffset++] = (int) ((block4 >> 40) & 1048575L);
    values[valuesOffset++] = (int) ((block4 >> 20) & 1048575L);
    values[valuesOffset++] = (int) (block4 & 1048575L);
  }
}

static void decode21(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 2; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 43);
    values[valuesOffset++] = (int) ((block0 >> 22) & 2097151L);
    values[valuesOffset++] = (int) ((block0 >> 1) & 2097151L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 1L) << 20) | (block1 >> 44));
    values[valuesOffset++] = (int) ((block1 >> 23) & 2097151L);
    values[valuesOffset++] = (int) ((block1 >> 2) & 2097151L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 3L) << 19) | (block2 >> 45));
    values[valuesOffset++] = (int) ((block2 >> 24) & 2097151L);
    values[valuesOffset++] = (int) ((block2 >> 3) & 2097151L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 7L) << 18) | (block3 >> 46));
    values[valuesOffset++] = (int) ((block3 >> 25) & 2097151L);
    values[valuesOffset++] = (int) ((block3 >> 4) & 2097151L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 15L) << 17) | (block4 >> 47));
    values[valuesOffset++] = (int) ((block4 >> 26) & 2097151L);
    values[valuesOffset++] = (int) ((block4 >> 5) & 2097151L);
    unsigned long block5 = blocks[blocksOffset++];
    block5 = __bswap_64(block5);
    values[valuesOffset++] = (int) (((block4 & 31L) << 16) | (block5 >> 48));
    values[valuesOffset++] = (int) ((block5 >> 27) & 2097151L);
    values[valuesOffset++] = (int) ((block5 >> 6) & 2097151L);
    unsigned long block6 = blocks[blocksOffset++];
    block6 = __bswap_64(block6);
    values[valuesOffset++] = (int) (((block5 & 63L) << 15) | (block6 >> 49));
    values[valuesOffset++] = (int) ((block6 >> 28) & 2097151L);
    values[valuesOffset++] = (int) ((block6 >> 7) & 2097151L);
    unsigned long block7 = blocks[blocksOffset++];
    block7 = __bswap_64(block7);
    values[valuesOffset++] = (int) (((block6 & 127L) << 14) | (block7 >> 50));
    values[valuesOffset++] = (int) ((block7 >> 29) & 2097151L);
    values[valuesOffset++] = (int) ((block7 >> 8) & 2097151L);
    unsigned long block8 = blocks[blocksOffset++];
    block8 = __bswap_64(block8);
    values[valuesOffset++] = (int) (((block7 & 255L) << 13) | (block8 >> 51));
    values[valuesOffset++] = (int) ((block8 >> 30) & 2097151L);
    values[valuesOffset++] = (int) ((block8 >> 9) & 2097151L);
    unsigned long block9 = blocks[blocksOffset++];
    block9 = __bswap_64(block9);
    values[valuesOffset++] = (int) (((block8 & 511L) << 12) | (block9 >> 52));
    values[valuesOffset++] = (int) ((block9 >> 31) & 2097151L);
    values[valuesOffset++] = (int) ((block9 >> 10) & 2097151L);
    unsigned long block10 = blocks[blocksOffset++];
    block10 = __bswap_64(block10);
    values[valuesOffset++] = (int) (((block9 & 1023L) << 11) | (block10 >> 53));
    values[valuesOffset++] = (int) ((block10 >> 32) & 2097151L);
    values[valuesOffset++] = (int) ((block10 >> 11) & 2097151L);
    unsigned long block11 = blocks[blocksOffset++];
    block11 = __bswap_64(block11);
    values[valuesOffset++] = (int) (((block10 & 2047L) << 10) | (block11 >> 54));
    values[valuesOffset++] = (int) ((block11 >> 33) & 2097151L);
    values[valuesOffset++] = (int) ((block11 >> 12) & 2097151L);
    unsigned long block12 = blocks[blocksOffset++];
    block12 = __bswap_64(block12);
    values[valuesOffset++] = (int) (((block11 & 4095L) << 9) | (block12 >> 55));
    values[valuesOffset++] = (int) ((block12 >> 34) & 2097151L);
    values[valuesOffset++] = (int) ((block12 >> 13) & 2097151L);
    unsigned long block13 = blocks[blocksOffset++];
    block13 = __bswap_64(block13);
    values[valuesOffset++] = (int) (((block12 & 8191L) << 8) | (block13 >> 56));
    values[valuesOffset++] = (int) ((block13 >> 35) & 2097151L);
    values[valuesOffset++] = (int) ((block13 >> 14) & 2097151L);
    unsigned long block14 = blocks[blocksOffset++];
    block14 = __bswap_64(block14);
    values[valuesOffset++] = (int) (((block13 & 16383L) << 7) | (block14 >> 57));
    values[valuesOffset++] = (int) ((block14 >> 36) & 2097151L);
    values[valuesOffset++] = (int) ((block14 >> 15) & 2097151L);
    unsigned long block15 = blocks[blocksOffset++];
    block15 = __bswap_64(block15);
    values[valuesOffset++] = (int) (((block14 & 32767L) << 6) | (block15 >> 58));
    values[valuesOffset++] = (int) ((block15 >> 37) & 2097151L);
    values[valuesOffset++] = (int) ((block15 >> 16) & 2097151L);
    unsigned long block16 = blocks[blocksOffset++];
    block16 = __bswap_64(block16);
    values[valuesOffset++] = (int) (((block15 & 65535L) << 5) | (block16 >> 59));
    values[valuesOffset++] = (int) ((block16 >> 38) & 2097151L);
    values[valuesOffset++] = (int) ((block16 >> 17) & 2097151L);
    unsigned long block17 = blocks[blocksOffset++];
    block17 = __bswap_64(block17);
    values[valuesOffset++] = (int) (((block16 & 131071L) << 4) | (block17 >> 60));
    values[valuesOffset++] = (int) ((block17 >> 39) & 2097151L);
    values[valuesOffset++] = (int) ((block17 >> 18) & 2097151L);
    unsigned long block18 = blocks[blocksOffset++];
    block18 = __bswap_64(block18);
    values[valuesOffset++] = (int) (((block17 & 262143L) << 3) | (block18 >> 61));
    values[valuesOffset++] = (int) ((block18 >> 40) & 2097151L);
    values[valuesOffset++] = (int) ((block18 >> 19) & 2097151L);
    unsigned long block19 = blocks[blocksOffset++];
    block19 = __bswap_64(block19);
    values[valuesOffset++] = (int) (((block18 & 524287L) << 2) | (block19 >> 62));
    values[valuesOffset++] = (int) ((block19 >> 41) & 2097151L);
    values[valuesOffset++] = (int) ((block19 >> 20) & 2097151L);
    unsigned long block20 = blocks[blocksOffset++];
    block20 = __bswap_64(block20);
    values[valuesOffset++] = (int) (((block19 & 1048575L) << 1) | (block20 >> 63));
    values[valuesOffset++] = (int) ((block20 >> 42) & 2097151L);
    values[valuesOffset++] = (int) ((block20 >> 21) & 2097151L);
    values[valuesOffset++] = (int) (block20 & 2097151L);
  }
}

static void decode22(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 4; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 42);
    values[valuesOffset++] = (int) ((block0 >> 20) & 4194303L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 1048575L) << 2) | (block1 >> 62));
    values[valuesOffset++] = (int) ((block1 >> 40) & 4194303L);
    values[valuesOffset++] = (int) ((block1 >> 18) & 4194303L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 262143L) << 4) | (block2 >> 60));
    values[valuesOffset++] = (int) ((block2 >> 38) & 4194303L);
    values[valuesOffset++] = (int) ((block2 >> 16) & 4194303L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 65535L) << 6) | (block3 >> 58));
    values[valuesOffset++] = (int) ((block3 >> 36) & 4194303L);
    values[valuesOffset++] = (int) ((block3 >> 14) & 4194303L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 16383L) << 8) | (block4 >> 56));
    values[valuesOffset++] = (int) ((block4 >> 34) & 4194303L);
    values[valuesOffset++] = (int) ((block4 >> 12) & 4194303L);
    unsigned long block5 = blocks[blocksOffset++];
    block5 = __bswap_64(block5);
    values[valuesOffset++] = (int) (((block4 & 4095L) << 10) | (block5 >> 54));
    values[valuesOffset++] = (int) ((block5 >> 32) & 4194303L);
    values[valuesOffset++] = (int) ((block5 >> 10) & 4194303L);
    unsigned long block6 = blocks[blocksOffset++];
    block6 = __bswap_64(block6);
    values[valuesOffset++] = (int) (((block5 & 1023L) << 12) | (block6 >> 52));
    values[valuesOffset++] = (int) ((block6 >> 30) & 4194303L);
    values[valuesOffset++] = (int) ((block6 >> 8) & 4194303L);
    unsigned long block7 = blocks[blocksOffset++];
    block7 = __bswap_64(block7);
    values[valuesOffset++] = (int) (((block6 & 255L) << 14) | (block7 >> 50));
    values[valuesOffset++] = (int) ((block7 >> 28) & 4194303L);
    values[valuesOffset++] = (int) ((block7 >> 6) & 4194303L);
    unsigned long block8 = blocks[blocksOffset++];
    block8 = __bswap_64(block8);
    values[valuesOffset++] = (int) (((block7 & 63L) << 16) | (block8 >> 48));
    values[valuesOffset++] = (int) ((block8 >> 26) & 4194303L);
    values[valuesOffset++] = (int) ((block8 >> 4) & 4194303L);
    unsigned long block9 = blocks[blocksOffset++];
    block9 = __bswap_64(block9);
    values[valuesOffset++] = (int) (((block8 & 15L) << 18) | (block9 >> 46));
    values[valuesOffset++] = (int) ((block9 >> 24) & 4194303L);
    values[valuesOffset++] = (int) ((block9 >> 2) & 4194303L);
    unsigned long block10 = blocks[blocksOffset++];
    block10 = __bswap_64(block10);
    values[valuesOffset++] = (int) (((block9 & 3L) << 20) | (block10 >> 44));
    values[valuesOffset++] = (int) ((block10 >> 22) & 4194303L);
    values[valuesOffset++] = (int) (block10 & 4194303L);
  }
}

static void decode23(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 2; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 41);
    values[valuesOffset++] = (int) ((block0 >> 18) & 8388607L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 262143L) << 5) | (block1 >> 59));
    values[valuesOffset++] = (int) ((block1 >> 36) & 8388607L);
    values[valuesOffset++] = (int) ((block1 >> 13) & 8388607L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 8191L) << 10) | (block2 >> 54));
    values[valuesOffset++] = (int) ((block2 >> 31) & 8388607L);
    values[valuesOffset++] = (int) ((block2 >> 8) & 8388607L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 255L) << 15) | (block3 >> 49));
    values[valuesOffset++] = (int) ((block3 >> 26) & 8388607L);
    values[valuesOffset++] = (int) ((block3 >> 3) & 8388607L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 7L) << 20) | (block4 >> 44));
    values[valuesOffset++] = (int) ((block4 >> 21) & 8388607L);
    unsigned long block5 = blocks[blocksOffset++];
    block5 = __bswap_64(block5);
    values[valuesOffset++] = (int) (((block4 & 2097151L) << 2) | (block5 >> 62));
    values[valuesOffset++] = (int) ((block5 >> 39) & 8388607L);
    values[valuesOffset++] = (int) ((block5 >> 16) & 8388607L);
    unsigned long block6 = blocks[blocksOffset++];
    block6 = __bswap_64(block6);
    values[valuesOffset++] = (int) (((block5 & 65535L) << 7) | (block6 >> 57));
    values[valuesOffset++] = (int) ((block6 >> 34) & 8388607L);
    values[valuesOffset++] = (int) ((block6 >> 11) & 8388607L);
    unsigned long block7 = blocks[blocksOffset++];
    block7 = __bswap_64(block7);
    values[valuesOffset++] = (int) (((block6 & 2047L) << 12) | (block7 >> 52));
    values[valuesOffset++] = (int) ((block7 >> 29) & 8388607L);
    values[valuesOffset++] = (int) ((block7 >> 6) & 8388607L);
    unsigned long block8 = blocks[blocksOffset++];
    block8 = __bswap_64(block8);
    values[valuesOffset++] = (int) (((block7 & 63L) << 17) | (block8 >> 47));
    values[valuesOffset++] = (int) ((block8 >> 24) & 8388607L);
    values[valuesOffset++] = (int) ((block8 >> 1) & 8388607L);
    unsigned long block9 = blocks[blocksOffset++];
    block9 = __bswap_64(block9);
    values[valuesOffset++] = (int) (((block8 & 1L) << 22) | (block9 >> 42));
    values[valuesOffset++] = (int) ((block9 >> 19) & 8388607L);
    unsigned long block10 = blocks[blocksOffset++];
    block10 = __bswap_64(block10);
    values[valuesOffset++] = (int) (((block9 & 524287L) << 4) | (block10 >> 60));
    values[valuesOffset++] = (int) ((block10 >> 37) & 8388607L);
    values[valuesOffset++] = (int) ((block10 >> 14) & 8388607L);
    unsigned long block11 = blocks[blocksOffset++];
    block11 = __bswap_64(block11);
    values[valuesOffset++] = (int) (((block10 & 16383L) << 9) | (block11 >> 55));
    values[valuesOffset++] = (int) ((block11 >> 32) & 8388607L);
    values[valuesOffset++] = (int) ((block11 >> 9) & 8388607L);
    unsigned long block12 = blocks[blocksOffset++];
    block12 = __bswap_64(block12);
    values[valuesOffset++] = (int) (((block11 & 511L) << 14) | (block12 >> 50));
    values[valuesOffset++] = (int) ((block12 >> 27) & 8388607L);
    values[valuesOffset++] = (int) ((block12 >> 4) & 8388607L);
    unsigned long block13 = blocks[blocksOffset++];
    block13 = __bswap_64(block13);
    values[valuesOffset++] = (int) (((block12 & 15L) << 19) | (block13 >> 45));
    values[valuesOffset++] = (int) ((block13 >> 22) & 8388607L);
    unsigned long block14 = blocks[blocksOffset++];
    block14 = __bswap_64(block14);
    values[valuesOffset++] = (int) (((block13 & 4194303L) << 1) | (block14 >> 63));
    values[valuesOffset++] = (int) ((block14 >> 40) & 8388607L);
    values[valuesOffset++] = (int) ((block14 >> 17) & 8388607L);
    unsigned long block15 = blocks[blocksOffset++];
    block15 = __bswap_64(block15);
    values[valuesOffset++] = (int) (((block14 & 131071L) << 6) | (block15 >> 58));
    values[valuesOffset++] = (int) ((block15 >> 35) & 8388607L);
    values[valuesOffset++] = (int) ((block15 >> 12) & 8388607L);
    unsigned long block16 = blocks[blocksOffset++];
    block16 = __bswap_64(block16);
    values[valuesOffset++] = (int) (((block15 & 4095L) << 11) | (block16 >> 53));
    values[valuesOffset++] = (int) ((block16 >> 30) & 8388607L);
    values[valuesOffset++] = (int) ((block16 >> 7) & 8388607L);
    unsigned long block17 = blocks[blocksOffset++];
    block17 = __bswap_64(block17);
    values[valuesOffset++] = (int) (((block16 & 127L) << 16) | (block17 >> 48));
    values[valuesOffset++] = (int) ((block17 >> 25) & 8388607L);
    values[valuesOffset++] = (int) ((block17 >> 2) & 8388607L);
    unsigned long block18 = blocks[blocksOffset++];
    block18 = __bswap_64(block18);
    values[valuesOffset++] = (int) (((block17 & 3L) << 21) | (block18 >> 43));
    values[valuesOffset++] = (int) ((block18 >> 20) & 8388607L);
    unsigned long block19 = blocks[blocksOffset++];
    block19 = __bswap_64(block19);
    values[valuesOffset++] = (int) (((block18 & 1048575L) << 3) | (block19 >> 61));
    values[valuesOffset++] = (int) ((block19 >> 38) & 8388607L);
    values[valuesOffset++] = (int) ((block19 >> 15) & 8388607L);
    unsigned long block20 = blocks[blocksOffset++];
    block20 = __bswap_64(block20);
    values[valuesOffset++] = (int) (((block19 & 32767L) << 8) | (block20 >> 56));
    values[valuesOffset++] = (int) ((block20 >> 33) & 8388607L);
    values[valuesOffset++] = (int) ((block20 >> 10) & 8388607L);
    unsigned long block21 = blocks[blocksOffset++];
    block21 = __bswap_64(block21);
    values[valuesOffset++] = (int) (((block20 & 1023L) << 13) | (block21 >> 51));
    values[valuesOffset++] = (int) ((block21 >> 28) & 8388607L);
    values[valuesOffset++] = (int) ((block21 >> 5) & 8388607L);
    unsigned long block22 = blocks[blocksOffset++];
    block22 = __bswap_64(block22);
    values[valuesOffset++] = (int) (((block21 & 31L) << 18) | (block22 >> 46));
    values[valuesOffset++] = (int) ((block22 >> 23) & 8388607L);
    values[valuesOffset++] = (int) (block22 & 8388607L);
  }
}

static void decode24(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 16; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 40);
    values[valuesOffset++] = (int) ((block0 >> 16) & 16777215L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 65535L) << 8) | (block1 >> 56));
    values[valuesOffset++] = (int) ((block1 >> 32) & 16777215L);
    values[valuesOffset++] = (int) ((block1 >> 8) & 16777215L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 255L) << 16) | (block2 >> 48));
    values[valuesOffset++] = (int) ((block2 >> 24) & 16777215L);
    values[valuesOffset++] = (int) (block2 & 16777215L);
  }
}

static void decode25(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 2; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 39);
    values[valuesOffset++] = (int) ((block0 >> 14) & 33554431L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 16383L) << 11) | (block1 >> 53));
    values[valuesOffset++] = (int) ((block1 >> 28) & 33554431L);
    values[valuesOffset++] = (int) ((block1 >> 3) & 33554431L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 7L) << 22) | (block2 >> 42));
    values[valuesOffset++] = (int) ((block2 >> 17) & 33554431L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 131071L) << 8) | (block3 >> 56));
    values[valuesOffset++] = (int) ((block3 >> 31) & 33554431L);
    values[valuesOffset++] = (int) ((block3 >> 6) & 33554431L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 63L) << 19) | (block4 >> 45));
    values[valuesOffset++] = (int) ((block4 >> 20) & 33554431L);
    unsigned long block5 = blocks[blocksOffset++];
    block5 = __bswap_64(block5);
    values[valuesOffset++] = (int) (((block4 & 1048575L) << 5) | (block5 >> 59));
    values[valuesOffset++] = (int) ((block5 >> 34) & 33554431L);
    values[valuesOffset++] = (int) ((block5 >> 9) & 33554431L);
    unsigned long block6 = blocks[blocksOffset++];
    block6 = __bswap_64(block6);
    values[valuesOffset++] = (int) (((block5 & 511L) << 16) | (block6 >> 48));
    values[valuesOffset++] = (int) ((block6 >> 23) & 33554431L);
    unsigned long block7 = blocks[blocksOffset++];
    block7 = __bswap_64(block7);
    values[valuesOffset++] = (int) (((block6 & 8388607L) << 2) | (block7 >> 62));
    values[valuesOffset++] = (int) ((block7 >> 37) & 33554431L);
    values[valuesOffset++] = (int) ((block7 >> 12) & 33554431L);
    unsigned long block8 = blocks[blocksOffset++];
    block8 = __bswap_64(block8);
    values[valuesOffset++] = (int) (((block7 & 4095L) << 13) | (block8 >> 51));
    values[valuesOffset++] = (int) ((block8 >> 26) & 33554431L);
    values[valuesOffset++] = (int) ((block8 >> 1) & 33554431L);
    unsigned long block9 = blocks[blocksOffset++];
    block9 = __bswap_64(block9);
    values[valuesOffset++] = (int) (((block8 & 1L) << 24) | (block9 >> 40));
    values[valuesOffset++] = (int) ((block9 >> 15) & 33554431L);
    unsigned long block10 = blocks[blocksOffset++];
    block10 = __bswap_64(block10);
    values[valuesOffset++] = (int) (((block9 & 32767L) << 10) | (block10 >> 54));
    values[valuesOffset++] = (int) ((block10 >> 29) & 33554431L);
    values[valuesOffset++] = (int) ((block10 >> 4) & 33554431L);
    unsigned long block11 = blocks[blocksOffset++];
    block11 = __bswap_64(block11);
    values[valuesOffset++] = (int) (((block10 & 15L) << 21) | (block11 >> 43));
    values[valuesOffset++] = (int) ((block11 >> 18) & 33554431L);
    unsigned long block12 = blocks[blocksOffset++];
    block12 = __bswap_64(block12);
    values[valuesOffset++] = (int) (((block11 & 262143L) << 7) | (block12 >> 57));
    values[valuesOffset++] = (int) ((block12 >> 32) & 33554431L);
    values[valuesOffset++] = (int) ((block12 >> 7) & 33554431L);
    unsigned long block13 = blocks[blocksOffset++];
    block13 = __bswap_64(block13);
    values[valuesOffset++] = (int) (((block12 & 127L) << 18) | (block13 >> 46));
    values[valuesOffset++] = (int) ((block13 >> 21) & 33554431L);
    unsigned long block14 = blocks[blocksOffset++];
    block14 = __bswap_64(block14);
    values[valuesOffset++] = (int) (((block13 & 2097151L) << 4) | (block14 >> 60));
    values[valuesOffset++] = (int) ((block14 >> 35) & 33554431L);
    values[valuesOffset++] = (int) ((block14 >> 10) & 33554431L);
    unsigned long block15 = blocks[blocksOffset++];
    block15 = __bswap_64(block15);
    values[valuesOffset++] = (int) (((block14 & 1023L) << 15) | (block15 >> 49));
    values[valuesOffset++] = (int) ((block15 >> 24) & 33554431L);
    unsigned long block16 = blocks[blocksOffset++];
    block16 = __bswap_64(block16);
    values[valuesOffset++] = (int) (((block15 & 16777215L) << 1) | (block16 >> 63));
    values[valuesOffset++] = (int) ((block16 >> 38) & 33554431L);
    values[valuesOffset++] = (int) ((block16 >> 13) & 33554431L);
    unsigned long block17 = blocks[blocksOffset++];
    block17 = __bswap_64(block17);
    values[valuesOffset++] = (int) (((block16 & 8191L) << 12) | (block17 >> 52));
    values[valuesOffset++] = (int) ((block17 >> 27) & 33554431L);
    values[valuesOffset++] = (int) ((block17 >> 2) & 33554431L);
    unsigned long block18 = blocks[blocksOffset++];
    block18 = __bswap_64(block18);
    values[valuesOffset++] = (int) (((block17 & 3L) << 23) | (block18 >> 41));
    values[valuesOffset++] = (int) ((block18 >> 16) & 33554431L);
    unsigned long block19 = blocks[blocksOffset++];
    block19 = __bswap_64(block19);
    values[valuesOffset++] = (int) (((block18 & 65535L) << 9) | (block19 >> 55));
    values[valuesOffset++] = (int) ((block19 >> 30) & 33554431L);
    values[valuesOffset++] = (int) ((block19 >> 5) & 33554431L);
    unsigned long block20 = blocks[blocksOffset++];
    block20 = __bswap_64(block20);
    values[valuesOffset++] = (int) (((block19 & 31L) << 20) | (block20 >> 44));
    values[valuesOffset++] = (int) ((block20 >> 19) & 33554431L);
    unsigned long block21 = blocks[blocksOffset++];
    block21 = __bswap_64(block21);
    values[valuesOffset++] = (int) (((block20 & 524287L) << 6) | (block21 >> 58));
    values[valuesOffset++] = (int) ((block21 >> 33) & 33554431L);
    values[valuesOffset++] = (int) ((block21 >> 8) & 33554431L);
    unsigned long block22 = blocks[blocksOffset++];
    block22 = __bswap_64(block22);
    values[valuesOffset++] = (int) (((block21 & 255L) << 17) | (block22 >> 47));
    values[valuesOffset++] = (int) ((block22 >> 22) & 33554431L);
    unsigned long block23 = blocks[blocksOffset++];
    block23 = __bswap_64(block23);
    values[valuesOffset++] = (int) (((block22 & 4194303L) << 3) | (block23 >> 61));
    values[valuesOffset++] = (int) ((block23 >> 36) & 33554431L);
    values[valuesOffset++] = (int) ((block23 >> 11) & 33554431L);
    unsigned long block24 = blocks[blocksOffset++];
    block24 = __bswap_64(block24);
    values[valuesOffset++] = (int) (((block23 & 2047L) << 14) | (block24 >> 50));
    values[valuesOffset++] = (int) ((block24 >> 25) & 33554431L);
    values[valuesOffset++] = (int) (block24 & 33554431L);
  }
}

static void decode26(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 4; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 38);
    values[valuesOffset++] = (int) ((block0 >> 12) & 67108863L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 4095L) << 14) | (block1 >> 50));
    values[valuesOffset++] = (int) ((block1 >> 24) & 67108863L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 16777215L) << 2) | (block2 >> 62));
    values[valuesOffset++] = (int) ((block2 >> 36) & 67108863L);
    values[valuesOffset++] = (int) ((block2 >> 10) & 67108863L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 1023L) << 16) | (block3 >> 48));
    values[valuesOffset++] = (int) ((block3 >> 22) & 67108863L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 4194303L) << 4) | (block4 >> 60));
    values[valuesOffset++] = (int) ((block4 >> 34) & 67108863L);
    values[valuesOffset++] = (int) ((block4 >> 8) & 67108863L);
    unsigned long block5 = blocks[blocksOffset++];
    block5 = __bswap_64(block5);
    values[valuesOffset++] = (int) (((block4 & 255L) << 18) | (block5 >> 46));
    values[valuesOffset++] = (int) ((block5 >> 20) & 67108863L);
    unsigned long block6 = blocks[blocksOffset++];
    block6 = __bswap_64(block6);
    values[valuesOffset++] = (int) (((block5 & 1048575L) << 6) | (block6 >> 58));
    values[valuesOffset++] = (int) ((block6 >> 32) & 67108863L);
    values[valuesOffset++] = (int) ((block6 >> 6) & 67108863L);
    unsigned long block7 = blocks[blocksOffset++];
    block7 = __bswap_64(block7);
    values[valuesOffset++] = (int) (((block6 & 63L) << 20) | (block7 >> 44));
    values[valuesOffset++] = (int) ((block7 >> 18) & 67108863L);
    unsigned long block8 = blocks[blocksOffset++];
    block8 = __bswap_64(block8);
    values[valuesOffset++] = (int) (((block7 & 262143L) << 8) | (block8 >> 56));
    values[valuesOffset++] = (int) ((block8 >> 30) & 67108863L);
    values[valuesOffset++] = (int) ((block8 >> 4) & 67108863L);
    unsigned long block9 = blocks[blocksOffset++];
    block9 = __bswap_64(block9);
    values[valuesOffset++] = (int) (((block8 & 15L) << 22) | (block9 >> 42));
    values[valuesOffset++] = (int) ((block9 >> 16) & 67108863L);
    unsigned long block10 = blocks[blocksOffset++];
    block10 = __bswap_64(block10);
    values[valuesOffset++] = (int) (((block9 & 65535L) << 10) | (block10 >> 54));
    values[valuesOffset++] = (int) ((block10 >> 28) & 67108863L);
    values[valuesOffset++] = (int) ((block10 >> 2) & 67108863L);
    unsigned long block11 = blocks[blocksOffset++];
    block11 = __bswap_64(block11);
    values[valuesOffset++] = (int) (((block10 & 3L) << 24) | (block11 >> 40));
    values[valuesOffset++] = (int) ((block11 >> 14) & 67108863L);
    unsigned long block12 = blocks[blocksOffset++];
    block12 = __bswap_64(block12);
    values[valuesOffset++] = (int) (((block11 & 16383L) << 12) | (block12 >> 52));
    values[valuesOffset++] = (int) ((block12 >> 26) & 67108863L);
    values[valuesOffset++] = (int) (block12 & 67108863L);
  }
}

static void decode27(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 2; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 37);
    values[valuesOffset++] = (int) ((block0 >> 10) & 134217727L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 1023L) << 17) | (block1 >> 47));
    values[valuesOffset++] = (int) ((block1 >> 20) & 134217727L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 1048575L) << 7) | (block2 >> 57));
    values[valuesOffset++] = (int) ((block2 >> 30) & 134217727L);
    values[valuesOffset++] = (int) ((block2 >> 3) & 134217727L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 7L) << 24) | (block3 >> 40));
    values[valuesOffset++] = (int) ((block3 >> 13) & 134217727L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 8191L) << 14) | (block4 >> 50));
    values[valuesOffset++] = (int) ((block4 >> 23) & 134217727L);
    unsigned long block5 = blocks[blocksOffset++];
    block5 = __bswap_64(block5);
    values[valuesOffset++] = (int) (((block4 & 8388607L) << 4) | (block5 >> 60));
    values[valuesOffset++] = (int) ((block5 >> 33) & 134217727L);
    values[valuesOffset++] = (int) ((block5 >> 6) & 134217727L);
    unsigned long block6 = blocks[blocksOffset++];
    block6 = __bswap_64(block6);
    values[valuesOffset++] = (int) (((block5 & 63L) << 21) | (block6 >> 43));
    values[valuesOffset++] = (int) ((block6 >> 16) & 134217727L);
    unsigned long block7 = blocks[blocksOffset++];
    block7 = __bswap_64(block7);
    values[valuesOffset++] = (int) (((block6 & 65535L) << 11) | (block7 >> 53));
    values[valuesOffset++] = (int) ((block7 >> 26) & 134217727L);
    unsigned long block8 = blocks[blocksOffset++];
    block8 = __bswap_64(block8);
    values[valuesOffset++] = (int) (((block7 & 67108863L) << 1) | (block8 >> 63));
    values[valuesOffset++] = (int) ((block8 >> 36) & 134217727L);
    values[valuesOffset++] = (int) ((block8 >> 9) & 134217727L);
    unsigned long block9 = blocks[blocksOffset++];
    block9 = __bswap_64(block9);
    values[valuesOffset++] = (int) (((block8 & 511L) << 18) | (block9 >> 46));
    values[valuesOffset++] = (int) ((block9 >> 19) & 134217727L);
    unsigned long block10 = blocks[blocksOffset++];
    block10 = __bswap_64(block10);
    values[valuesOffset++] = (int) (((block9 & 524287L) << 8) | (block10 >> 56));
    values[valuesOffset++] = (int) ((block10 >> 29) & 134217727L);
    values[valuesOffset++] = (int) ((block10 >> 2) & 134217727L);
    unsigned long block11 = blocks[blocksOffset++];
    block11 = __bswap_64(block11);
    values[valuesOffset++] = (int) (((block10 & 3L) << 25) | (block11 >> 39));
    values[valuesOffset++] = (int) ((block11 >> 12) & 134217727L);
    unsigned long block12 = blocks[blocksOffset++];
    block12 = __bswap_64(block12);
    values[valuesOffset++] = (int) (((block11 & 4095L) << 15) | (block12 >> 49));
    values[valuesOffset++] = (int) ((block12 >> 22) & 134217727L);
    unsigned long block13 = blocks[blocksOffset++];
    block13 = __bswap_64(block13);
    values[valuesOffset++] = (int) (((block12 & 4194303L) << 5) | (block13 >> 59));
    values[valuesOffset++] = (int) ((block13 >> 32) & 134217727L);
    values[valuesOffset++] = (int) ((block13 >> 5) & 134217727L);
    unsigned long block14 = blocks[blocksOffset++];
    block14 = __bswap_64(block14);
    values[valuesOffset++] = (int) (((block13 & 31L) << 22) | (block14 >> 42));
    values[valuesOffset++] = (int) ((block14 >> 15) & 134217727L);
    unsigned long block15 = blocks[blocksOffset++];
    block15 = __bswap_64(block15);
    values[valuesOffset++] = (int) (((block14 & 32767L) << 12) | (block15 >> 52));
    values[valuesOffset++] = (int) ((block15 >> 25) & 134217727L);
    unsigned long block16 = blocks[blocksOffset++];
    block16 = __bswap_64(block16);
    values[valuesOffset++] = (int) (((block15 & 33554431L) << 2) | (block16 >> 62));
    values[valuesOffset++] = (int) ((block16 >> 35) & 134217727L);
    values[valuesOffset++] = (int) ((block16 >> 8) & 134217727L);
    unsigned long block17 = blocks[blocksOffset++];
    block17 = __bswap_64(block17);
    values[valuesOffset++] = (int) (((block16 & 255L) << 19) | (block17 >> 45));
    values[valuesOffset++] = (int) ((block17 >> 18) & 134217727L);
    unsigned long block18 = blocks[blocksOffset++];
    block18 = __bswap_64(block18);
    values[valuesOffset++] = (int) (((block17 & 262143L) << 9) | (block18 >> 55));
    values[valuesOffset++] = (int) ((block18 >> 28) & 134217727L);
    values[valuesOffset++] = (int) ((block18 >> 1) & 134217727L);
    unsigned long block19 = blocks[blocksOffset++];
    block19 = __bswap_64(block19);
    values[valuesOffset++] = (int) (((block18 & 1L) << 26) | (block19 >> 38));
    values[valuesOffset++] = (int) ((block19 >> 11) & 134217727L);
    unsigned long block20 = blocks[blocksOffset++];
    block20 = __bswap_64(block20);
    values[valuesOffset++] = (int) (((block19 & 2047L) << 16) | (block20 >> 48));
    values[valuesOffset++] = (int) ((block20 >> 21) & 134217727L);
    unsigned long block21 = blocks[blocksOffset++];
    block21 = __bswap_64(block21);
    values[valuesOffset++] = (int) (((block20 & 2097151L) << 6) | (block21 >> 58));
    values[valuesOffset++] = (int) ((block21 >> 31) & 134217727L);
    values[valuesOffset++] = (int) ((block21 >> 4) & 134217727L);
    unsigned long block22 = blocks[blocksOffset++];
    block22 = __bswap_64(block22);
    values[valuesOffset++] = (int) (((block21 & 15L) << 23) | (block22 >> 41));
    values[valuesOffset++] = (int) ((block22 >> 14) & 134217727L);
    unsigned long block23 = blocks[blocksOffset++];
    block23 = __bswap_64(block23);
    values[valuesOffset++] = (int) (((block22 & 16383L) << 13) | (block23 >> 51));
    values[valuesOffset++] = (int) ((block23 >> 24) & 134217727L);
    unsigned long block24 = blocks[blocksOffset++];
    block24 = __bswap_64(block24);
    values[valuesOffset++] = (int) (((block23 & 16777215L) << 3) | (block24 >> 61));
    values[valuesOffset++] = (int) ((block24 >> 34) & 134217727L);
    values[valuesOffset++] = (int) ((block24 >> 7) & 134217727L);
    unsigned long block25 = blocks[blocksOffset++];
    block25 = __bswap_64(block25);
    values[valuesOffset++] = (int) (((block24 & 127L) << 20) | (block25 >> 44));
    values[valuesOffset++] = (int) ((block25 >> 17) & 134217727L);
    unsigned long block26 = blocks[blocksOffset++];
    block26 = __bswap_64(block26);
    values[valuesOffset++] = (int) (((block25 & 131071L) << 10) | (block26 >> 54));
    values[valuesOffset++] = (int) ((block26 >> 27) & 134217727L);
    values[valuesOffset++] = (int) (block26 & 134217727L);
  }
}

static void decode28(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 8; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 36);
    values[valuesOffset++] = (int) ((block0 >> 8) & 268435455L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 255L) << 20) | (block1 >> 44));
    values[valuesOffset++] = (int) ((block1 >> 16) & 268435455L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 65535L) << 12) | (block2 >> 52));
    values[valuesOffset++] = (int) ((block2 >> 24) & 268435455L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 16777215L) << 4) | (block3 >> 60));
    values[valuesOffset++] = (int) ((block3 >> 32) & 268435455L);
    values[valuesOffset++] = (int) ((block3 >> 4) & 268435455L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 15L) << 24) | (block4 >> 40));
    values[valuesOffset++] = (int) ((block4 >> 12) & 268435455L);
    unsigned long block5 = blocks[blocksOffset++];
    block5 = __bswap_64(block5);
    values[valuesOffset++] = (int) (((block4 & 4095L) << 16) | (block5 >> 48));
    values[valuesOffset++] = (int) ((block5 >> 20) & 268435455L);
    unsigned long block6 = blocks[blocksOffset++];
    block6 = __bswap_64(block6);
    values[valuesOffset++] = (int) (((block5 & 1048575L) << 8) | (block6 >> 56));
    values[valuesOffset++] = (int) ((block6 >> 28) & 268435455L);
    values[valuesOffset++] = (int) (block6 & 268435455L);
  }
}

static void decode29(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 2; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 35);
    values[valuesOffset++] = (int) ((block0 >> 6) & 536870911L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 63L) << 23) | (block1 >> 41));
    values[valuesOffset++] = (int) ((block1 >> 12) & 536870911L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 4095L) << 17) | (block2 >> 47));
    values[valuesOffset++] = (int) ((block2 >> 18) & 536870911L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 262143L) << 11) | (block3 >> 53));
    values[valuesOffset++] = (int) ((block3 >> 24) & 536870911L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 16777215L) << 5) | (block4 >> 59));
    values[valuesOffset++] = (int) ((block4 >> 30) & 536870911L);
    values[valuesOffset++] = (int) ((block4 >> 1) & 536870911L);
    unsigned long block5 = blocks[blocksOffset++];
    block5 = __bswap_64(block5);
    values[valuesOffset++] = (int) (((block4 & 1L) << 28) | (block5 >> 36));
    values[valuesOffset++] = (int) ((block5 >> 7) & 536870911L);
    unsigned long block6 = blocks[blocksOffset++];
    block6 = __bswap_64(block6);
    values[valuesOffset++] = (int) (((block5 & 127L) << 22) | (block6 >> 42));
    values[valuesOffset++] = (int) ((block6 >> 13) & 536870911L);
    unsigned long block7 = blocks[blocksOffset++];
    block7 = __bswap_64(block7);
    values[valuesOffset++] = (int) (((block6 & 8191L) << 16) | (block7 >> 48));
    values[valuesOffset++] = (int) ((block7 >> 19) & 536870911L);
    unsigned long block8 = blocks[blocksOffset++];
    block8 = __bswap_64(block8);
    values[valuesOffset++] = (int) (((block7 & 524287L) << 10) | (block8 >> 54));
    values[valuesOffset++] = (int) ((block8 >> 25) & 536870911L);
    unsigned long block9 = blocks[blocksOffset++];
    block9 = __bswap_64(block9);
    values[valuesOffset++] = (int) (((block8 & 33554431L) << 4) | (block9 >> 60));
    values[valuesOffset++] = (int) ((block9 >> 31) & 536870911L);
    values[valuesOffset++] = (int) ((block9 >> 2) & 536870911L);
    unsigned long block10 = blocks[blocksOffset++];
    block10 = __bswap_64(block10);
    values[valuesOffset++] = (int) (((block9 & 3L) << 27) | (block10 >> 37));
    values[valuesOffset++] = (int) ((block10 >> 8) & 536870911L);
    unsigned long block11 = blocks[blocksOffset++];
    block11 = __bswap_64(block11);
    values[valuesOffset++] = (int) (((block10 & 255L) << 21) | (block11 >> 43));
    values[valuesOffset++] = (int) ((block11 >> 14) & 536870911L);
    unsigned long block12 = blocks[blocksOffset++];
    block12 = __bswap_64(block12);
    values[valuesOffset++] = (int) (((block11 & 16383L) << 15) | (block12 >> 49));
    values[valuesOffset++] = (int) ((block12 >> 20) & 536870911L);
    unsigned long block13 = blocks[blocksOffset++];
    block13 = __bswap_64(block13);
    values[valuesOffset++] = (int) (((block12 & 1048575L) << 9) | (block13 >> 55));
    values[valuesOffset++] = (int) ((block13 >> 26) & 536870911L);
    unsigned long block14 = blocks[blocksOffset++];
    block14 = __bswap_64(block14);
    values[valuesOffset++] = (int) (((block13 & 67108863L) << 3) | (block14 >> 61));
    values[valuesOffset++] = (int) ((block14 >> 32) & 536870911L);
    values[valuesOffset++] = (int) ((block14 >> 3) & 536870911L);
    unsigned long block15 = blocks[blocksOffset++];
    block15 = __bswap_64(block15);
    values[valuesOffset++] = (int) (((block14 & 7L) << 26) | (block15 >> 38));
    values[valuesOffset++] = (int) ((block15 >> 9) & 536870911L);
    unsigned long block16 = blocks[blocksOffset++];
    block16 = __bswap_64(block16);
    values[valuesOffset++] = (int) (((block15 & 511L) << 20) | (block16 >> 44));
    values[valuesOffset++] = (int) ((block16 >> 15) & 536870911L);
    unsigned long block17 = blocks[blocksOffset++];
    block17 = __bswap_64(block17);
    values[valuesOffset++] = (int) (((block16 & 32767L) << 14) | (block17 >> 50));
    values[valuesOffset++] = (int) ((block17 >> 21) & 536870911L);
    unsigned long block18 = blocks[blocksOffset++];
    block18 = __bswap_64(block18);
    values[valuesOffset++] = (int) (((block17 & 2097151L) << 8) | (block18 >> 56));
    values[valuesOffset++] = (int) ((block18 >> 27) & 536870911L);
    unsigned long block19 = blocks[blocksOffset++];
    block19 = __bswap_64(block19);
    values[valuesOffset++] = (int) (((block18 & 134217727L) << 2) | (block19 >> 62));
    values[valuesOffset++] = (int) ((block19 >> 33) & 536870911L);
    values[valuesOffset++] = (int) ((block19 >> 4) & 536870911L);
    unsigned long block20 = blocks[blocksOffset++];
    block20 = __bswap_64(block20);
    values[valuesOffset++] = (int) (((block19 & 15L) << 25) | (block20 >> 39));
    values[valuesOffset++] = (int) ((block20 >> 10) & 536870911L);
    unsigned long block21 = blocks[blocksOffset++];
    block21 = __bswap_64(block21);
    values[valuesOffset++] = (int) (((block20 & 1023L) << 19) | (block21 >> 45));
    values[valuesOffset++] = (int) ((block21 >> 16) & 536870911L);
    unsigned long block22 = blocks[blocksOffset++];
    block22 = __bswap_64(block22);
    values[valuesOffset++] = (int) (((block21 & 65535L) << 13) | (block22 >> 51));
    values[valuesOffset++] = (int) ((block22 >> 22) & 536870911L);
    unsigned long block23 = blocks[blocksOffset++];
    block23 = __bswap_64(block23);
    values[valuesOffset++] = (int) (((block22 & 4194303L) << 7) | (block23 >> 57));
    values[valuesOffset++] = (int) ((block23 >> 28) & 536870911L);
    unsigned long block24 = blocks[blocksOffset++];
    block24 = __bswap_64(block24);
    values[valuesOffset++] = (int) (((block23 & 268435455L) << 1) | (block24 >> 63));
    values[valuesOffset++] = (int) ((block24 >> 34) & 536870911L);
    values[valuesOffset++] = (int) ((block24 >> 5) & 536870911L);
    unsigned long block25 = blocks[blocksOffset++];
    block25 = __bswap_64(block25);
    values[valuesOffset++] = (int) (((block24 & 31L) << 24) | (block25 >> 40));
    values[valuesOffset++] = (int) ((block25 >> 11) & 536870911L);
    unsigned long block26 = blocks[blocksOffset++];
    block26 = __bswap_64(block26);
    values[valuesOffset++] = (int) (((block25 & 2047L) << 18) | (block26 >> 46));
    values[valuesOffset++] = (int) ((block26 >> 17) & 536870911L);
    unsigned long block27 = blocks[blocksOffset++];
    block27 = __bswap_64(block27);
    values[valuesOffset++] = (int) (((block26 & 131071L) << 12) | (block27 >> 52));
    values[valuesOffset++] = (int) ((block27 >> 23) & 536870911L);
    unsigned long block28 = blocks[blocksOffset++];
    block28 = __bswap_64(block28);
    values[valuesOffset++] = (int) (((block27 & 8388607L) << 6) | (block28 >> 58));
    values[valuesOffset++] = (int) ((block28 >> 29) & 536870911L);
    values[valuesOffset++] = (int) (block28 & 536870911L);
  }
}

static void decode30(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 4; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 34);
    values[valuesOffset++] = (int) ((block0 >> 4) & 1073741823L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 15L) << 26) | (block1 >> 38));
    values[valuesOffset++] = (int) ((block1 >> 8) & 1073741823L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 255L) << 22) | (block2 >> 42));
    values[valuesOffset++] = (int) ((block2 >> 12) & 1073741823L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 4095L) << 18) | (block3 >> 46));
    values[valuesOffset++] = (int) ((block3 >> 16) & 1073741823L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 65535L) << 14) | (block4 >> 50));
    values[valuesOffset++] = (int) ((block4 >> 20) & 1073741823L);
    unsigned long block5 = blocks[blocksOffset++];
    block5 = __bswap_64(block5);
    values[valuesOffset++] = (int) (((block4 & 1048575L) << 10) | (block5 >> 54));
    values[valuesOffset++] = (int) ((block5 >> 24) & 1073741823L);
    unsigned long block6 = blocks[blocksOffset++];
    block6 = __bswap_64(block6);
    values[valuesOffset++] = (int) (((block5 & 16777215L) << 6) | (block6 >> 58));
    values[valuesOffset++] = (int) ((block6 >> 28) & 1073741823L);
    unsigned long block7 = blocks[blocksOffset++];
    block7 = __bswap_64(block7);
    values[valuesOffset++] = (int) (((block6 & 268435455L) << 2) | (block7 >> 62));
    values[valuesOffset++] = (int) ((block7 >> 32) & 1073741823L);
    values[valuesOffset++] = (int) ((block7 >> 2) & 1073741823L);
    unsigned long block8 = blocks[blocksOffset++];
    block8 = __bswap_64(block8);
    values[valuesOffset++] = (int) (((block7 & 3L) << 28) | (block8 >> 36));
    values[valuesOffset++] = (int) ((block8 >> 6) & 1073741823L);
    unsigned long block9 = blocks[blocksOffset++];
    block9 = __bswap_64(block9);
    values[valuesOffset++] = (int) (((block8 & 63L) << 24) | (block9 >> 40));
    values[valuesOffset++] = (int) ((block9 >> 10) & 1073741823L);
    unsigned long block10 = blocks[blocksOffset++];
    block10 = __bswap_64(block10);
    values[valuesOffset++] = (int) (((block9 & 1023L) << 20) | (block10 >> 44));
    values[valuesOffset++] = (int) ((block10 >> 14) & 1073741823L);
    unsigned long block11 = blocks[blocksOffset++];
    block11 = __bswap_64(block11);
    values[valuesOffset++] = (int) (((block10 & 16383L) << 16) | (block11 >> 48));
    values[valuesOffset++] = (int) ((block11 >> 18) & 1073741823L);
    unsigned long block12 = blocks[blocksOffset++];
    block12 = __bswap_64(block12);
    values[valuesOffset++] = (int) (((block11 & 262143L) << 12) | (block12 >> 52));
    values[valuesOffset++] = (int) ((block12 >> 22) & 1073741823L);
    unsigned long block13 = blocks[blocksOffset++];
    block13 = __bswap_64(block13);
    values[valuesOffset++] = (int) (((block12 & 4194303L) << 8) | (block13 >> 56));
    values[valuesOffset++] = (int) ((block13 >> 26) & 1073741823L);
    unsigned long block14 = blocks[blocksOffset++];
    block14 = __bswap_64(block14);
    values[valuesOffset++] = (int) (((block13 & 67108863L) << 4) | (block14 >> 60));
    values[valuesOffset++] = (int) ((block14 >> 30) & 1073741823L);
    values[valuesOffset++] = (int) (block14 & 1073741823L);
  }
}

static void decode31(unsigned long *blocks, unsigned int *values) {
  int blocksOffset = 0;
  int valuesOffset = 0;
  for (int i = 0; i < 2; ++i) {
    unsigned long block0 = blocks[blocksOffset++];
    block0 = __bswap_64(block0);
    values[valuesOffset++] = (int) (block0 >> 33);
    values[valuesOffset++] = (int) ((block0 >> 2) & 2147483647L);
    unsigned long block1 = blocks[blocksOffset++];
    block1 = __bswap_64(block1);
    values[valuesOffset++] = (int) (((block0 & 3L) << 29) | (block1 >> 35));
    values[valuesOffset++] = (int) ((block1 >> 4) & 2147483647L);
    unsigned long block2 = blocks[blocksOffset++];
    block2 = __bswap_64(block2);
    values[valuesOffset++] = (int) (((block1 & 15L) << 27) | (block2 >> 37));
    values[valuesOffset++] = (int) ((block2 >> 6) & 2147483647L);
    unsigned long block3 = blocks[blocksOffset++];
    block3 = __bswap_64(block3);
    values[valuesOffset++] = (int) (((block2 & 63L) << 25) | (block3 >> 39));
    values[valuesOffset++] = (int) ((block3 >> 8) & 2147483647L);
    unsigned long block4 = blocks[blocksOffset++];
    block4 = __bswap_64(block4);
    values[valuesOffset++] = (int) (((block3 & 255L) << 23) | (block4 >> 41));
    values[valuesOffset++] = (int) ((block4 >> 10) & 2147483647L);
    unsigned long block5 = blocks[blocksOffset++];
    block5 = __bswap_64(block5);
    values[valuesOffset++] = (int) (((block4 & 1023L) << 21) | (block5 >> 43));
    values[valuesOffset++] = (int) ((block5 >> 12) & 2147483647L);
    unsigned long block6 = blocks[blocksOffset++];
    block6 = __bswap_64(block6);
    values[valuesOffset++] = (int) (((block5 & 4095L) << 19) | (block6 >> 45));
    values[valuesOffset++] = (int) ((block6 >> 14) & 2147483647L);
    unsigned long block7 = blocks[blocksOffset++];
    block7 = __bswap_64(block7);
    values[valuesOffset++] = (int) (((block6 & 16383L) << 17) | (block7 >> 47));
    values[valuesOffset++] = (int) ((block7 >> 16) & 2147483647L);
    unsigned long block8 = blocks[blocksOffset++];
    block8 = __bswap_64(block8);
    values[valuesOffset++] = (int) (((block7 & 65535L) << 15) | (block8 >> 49));
    values[valuesOffset++] = (int) ((block8 >> 18) & 2147483647L);
    unsigned long block9 = blocks[blocksOffset++];
    block9 = __bswap_64(block9);
    values[valuesOffset++] = (int) (((block8 & 262143L) << 13) | (block9 >> 51));
    values[valuesOffset++] = (int) ((block9 >> 20) & 2147483647L);
    unsigned long block10 = blocks[blocksOffset++];
    block10 = __bswap_64(block10);
    values[valuesOffset++] = (int) (((block9 & 1048575L) << 11) | (block10 >> 53));
    values[valuesOffset++] = (int) ((block10 >> 22) & 2147483647L);
    unsigned long block11 = blocks[blocksOffset++];
    block11 = __bswap_64(block11);
    values[valuesOffset++] = (int) (((block10 & 4194303L) << 9) | (block11 >> 55));
    values[valuesOffset++] = (int) ((block11 >> 24) & 2147483647L);
    unsigned long block12 = blocks[blocksOffset++];
    block12 = __bswap_64(block12);
    values[valuesOffset++] = (int) (((block11 & 16777215L) << 7) | (block12 >> 57));
    values[valuesOffset++] = (int) ((block12 >> 26) & 2147483647L);
    unsigned long block13 = blocks[blocksOffset++];
    block13 = __bswap_64(block13);
    values[valuesOffset++] = (int) (((block12 & 67108863L) << 5) | (block13 >> 59));
    values[valuesOffset++] = (int) ((block13 >> 28) & 2147483647L);
    unsigned long block14 = blocks[blocksOffset++];
    block14 = __bswap_64(block14);
    values[valuesOffset++] = (int) (((block13 & 268435455L) << 3) | (block14 >> 61));
    values[valuesOffset++] = (int) ((block14 >> 30) & 2147483647L);
    unsigned long block15 = blocks[blocksOffset++];
    block15 = __bswap_64(block15);
    values[valuesOffset++] = (int) (((block14 & 1073741823L) << 1) | (block15 >> 63));
    values[valuesOffset++] = (int) ((block15 >> 32) & 2147483647L);
    values[valuesOffset++] = (int) ((block15 >> 1) & 2147483647L);
    unsigned long block16 = blocks[blocksOffset++];
    block16 = __bswap_64(block16);
    values[valuesOffset++] = (int) (((block15 & 1L) << 30) | (block16 >> 34));
    values[valuesOffset++] = (int) ((block16 >> 3) & 2147483647L);
    unsigned long block17 = blocks[blocksOffset++];
    block17 = __bswap_64(block17);
    values[valuesOffset++] = (int) (((block16 & 7L) << 28) | (block17 >> 36));
    values[valuesOffset++] = (int) ((block17 >> 5) & 2147483647L);
    unsigned long block18 = blocks[blocksOffset++];
    block18 = __bswap_64(block18);
    values[valuesOffset++] = (int) (((block17 & 31L) << 26) | (block18 >> 38));
    values[valuesOffset++] = (int) ((block18 >> 7) & 2147483647L);
    unsigned long block19 = blocks[blocksOffset++];
    block19 = __bswap_64(block19);
    values[valuesOffset++] = (int) (((block18 & 127L) << 24) | (block19 >> 40));
    values[valuesOffset++] = (int) ((block19 >> 9) & 2147483647L);
    unsigned long block20 = blocks[blocksOffset++];
    block20 = __bswap_64(block20);
    values[valuesOffset++] = (int) (((block19 & 511L) << 22) | (block20 >> 42));
    values[valuesOffset++] = (int) ((block20 >> 11) & 2147483647L);
    unsigned long block21 = blocks[blocksOffset++];
    block21 = __bswap_64(block21);
    values[valuesOffset++] = (int) (((block20 & 2047L) << 20) | (block21 >> 44));
    values[valuesOffset++] = (int) ((block21 >> 13) & 2147483647L);
    unsigned long block22 = blocks[blocksOffset++];
    block22 = __bswap_64(block22);
    values[valuesOffset++] = (int) (((block21 & 8191L) << 18) | (block22 >> 46));
    values[valuesOffset++] = (int) ((block22 >> 15) & 2147483647L);
    unsigned long block23 = blocks[blocksOffset++];
    block23 = __bswap_64(block23);
    values[valuesOffset++] = (int) (((block22 & 32767L) << 16) | (block23 >> 48));
    values[valuesOffset++] = (int) ((block23 >> 17) & 2147483647L);
    unsigned long block24 = blocks[blocksOffset++];
    block24 = __bswap_64(block24);
    values[valuesOffset++] = (int) (((block23 & 131071L) << 14) | (block24 >> 50));
    values[valuesOffset++] = (int) ((block24 >> 19) & 2147483647L);
    unsigned long block25 = blocks[blocksOffset++];
    block25 = __bswap_64(block25);
    values[valuesOffset++] = (int) (((block24 & 524287L) << 12) | (block25 >> 52));
    values[valuesOffset++] = (int) ((block25 >> 21) & 2147483647L);
    unsigned long block26 = blocks[blocksOffset++];
    block26 = __bswap_64(block26);
    values[valuesOffset++] = (int) (((block25 & 2097151L) << 10) | (block26 >> 54));
    values[valuesOffset++] = (int) ((block26 >> 23) & 2147483647L);
    unsigned long block27 = blocks[blocksOffset++];
    block27 = __bswap_64(block27);
    values[valuesOffset++] = (int) (((block26 & 8388607L) << 8) | (block27 >> 56));
    values[valuesOffset++] = (int) ((block27 >> 25) & 2147483647L);
    unsigned long block28 = blocks[blocksOffset++];
    block28 = __bswap_64(block28);
    values[valuesOffset++] = (int) (((block27 & 33554431L) << 6) | (block28 >> 58));
    values[valuesOffset++] = (int) ((block28 >> 27) & 2147483647L);
    unsigned long block29 = blocks[blocksOffset++];
    block29 = __bswap_64(block29);
    values[valuesOffset++] = (int) (((block28 & 134217727L) << 4) | (block29 >> 60));
    values[valuesOffset++] = (int) ((block29 >> 29) & 2147483647L);
    unsigned long block30 = blocks[blocksOffset++];
    block30 = __bswap_64(block30);
    values[valuesOffset++] = (int) (((block29 & 536870911L) << 2) | (block30 >> 62));
    values[valuesOffset++] = (int) ((block30 >> 31) & 2147483647L);
    values[valuesOffset++] = (int) (block30 & 2147483647L);
  }
}


static void readPackedBlock(unsigned long *longBuffer, PostingsState *sub, unsigned int *dest) {
  unsigned char bitsPerValue = readByte(sub);
  //printf("\nreadPackedBlock bpv=%d\n", bitsPerValue);
  if (bitsPerValue == 0) {
    // All values equal
    unsigned int v = readVInt(sub);
    for(int i=0;i<BLOCK_SIZE;i++) {
      dest[i] = v;
    }
  } else {
    int numBytes = bitsPerValue*16;
    //printf("\n  %d bytes @ p=%d\n", numBytes, (int) (sub->p - globalAddress));
    // Align to 8 bytes:
    //long x = (long) sub->p;
    //x = (x+7) & ~7;
    //sub->p = (unsigned char *) x;

    //memcpy(longBuffer, sub->p, numBytes);
    longBuffer = (unsigned long *) sub->p;
    sub->p += numBytes;

    // NOTE: Block PF uses PACKED_SINGLE_BLOCK for
    // bpv=1,2,4, else "ordinary" packed:
    switch(bitsPerValue) {
      case 1:
        decodeSingleBlock1(longBuffer, dest);
        break;
      case 2:
        decodeSingleBlock2(longBuffer, dest);
        break;
      case 3:
        decode3(longBuffer, dest);
        break;
      case 4:
        decodeSingleBlock4(longBuffer, dest);
        break;
      case 5:
        decode5(longBuffer, dest);
        break;
      case 6:
        decode6(longBuffer, dest);
        break;
      case 7:
        decode7(longBuffer, dest);
        break;
      case 8:
        decode8(longBuffer, dest);
        break;
      case 9:
        decode9(longBuffer, dest);
        break;
      case 10:
        decode10(longBuffer, dest);
        break;
      case 11:
        decode11(longBuffer, dest);
        break;
      case 12:
        decode12(longBuffer, dest);
        break;
      case 13:
        decode13(longBuffer, dest);
        break;
      case 14:
        decode14(longBuffer, dest);
        break;
      case 15:
        decode15(longBuffer, dest);
        break;
      case 16:
        decode16(longBuffer, dest);
        break;
      case 17:
        decode17(longBuffer, dest);
        break;
      case 18:
        decode18(longBuffer, dest);
        break;
      case 19:
        decode19(longBuffer, dest);
        break;
      case 20:
        decode20(longBuffer, dest);
        break;
      case 21:
        decode21(longBuffer, dest);
        break;
      case 22:
        decode22(longBuffer, dest);
        break;
      case 23:
        decode23(longBuffer, dest);
        break;
      case 24:
        decode24(longBuffer, dest);
        break;
      case 25:
        decode25(longBuffer, dest);
        break;
      case 26:
        decode26(longBuffer, dest);
        break;
      case 27:
        decode27(longBuffer, dest);
        break;
      case 28:
        decode28(longBuffer, dest);
        break;
      case 29:
        decode29(longBuffer, dest);
        break;
      case 30:
        decode30(longBuffer, dest);
        break;
      case 31:
        decode31(longBuffer, dest);
        break;

    }
  }
}
// END AUTOGEN CODE (gen_Packed.py)

static void skipPackedBlock(PostingsState *sub) {
  unsigned char bitsPerValue = readByte(sub);
  if (bitsPerValue == 0) {
    // All values equal
    readVInt(sub);
  } else {
    int numBytes = bitsPerValue*16;
    sub->p += numBytes;
  }
}

static void readVIntBlock(PostingsState *sub) {
  //printf("  readVIntBlock: %d docs\n", sub->docsLeft);
  if (sub->docsOnly) {
    for(int i=0;i<sub->docsLeft;i++) {
      sub->docDeltas[i] = readVInt(sub);
    }
  } else if (sub->freqs != 0) {
    for(int i=0;i<sub->docsLeft;i++) {
      unsigned int code = readVInt(sub);
      sub->docDeltas[i] = code >> 1;
      if ((code & 1) != 0) {
        sub->freqs[i] = 1;
      } else {
        sub->freqs[i] = readVInt(sub);
      }
      //printf("    docDeltas[%d] = %d\n", i, sub->docDeltas[i]);
      //printf("    freqs[%d] = %d\n", i, sub->freqs[i]);
    }
  } else {
    for(int i=0;i<sub->docsLeft;i++) {
      unsigned int code = readVInt(sub);
      sub->docDeltas[i] = code >> 1;
      if ((code & 1) == 0) {
        readVInt(sub);
      }
      //printf("    docDeltas[%d] = %d\n", i, sub->docDeltas[i]);
      //printf("    freqs[%d] = %d\n", i, sub->freqs[i]);
    }
  }
}

static void nextBlock(unsigned long *longBuffer, PostingsState* sub) {
  sub->blockLastRead = -1;
  if (sub->docsLeft >= BLOCK_SIZE) {
    //printf("  nextBlock: packed\n");
    readPackedBlock(longBuffer, sub, sub->docDeltas);
    if (!sub->docsOnly) {
      if (sub->freqs == 0) {
        skipPackedBlock(sub);
      } else {
        readPackedBlock(longBuffer, sub, sub->freqs);
      }
    }
    sub->docsLeft -= BLOCK_SIZE;
    // nocommit redundant?:  only needs to be done up front?
    sub->blockEnd = BLOCK_SIZE-1;
  } else {
    //printf("  nextBlock: vInt\n");
    sub->blockEnd = sub->docsLeft-1;
    readVIntBlock(sub);
    sub->docsLeft = 0;
  }
}

static bool
lessThan(int docID1, float score1, int docID2, float score2) {
  if (score1 < score2) {
    return true;
  } else if (score1 > score2) {
    return false;
  } else {
    if (docID1 > docID2) {
      return true;
    } else {
      return false;
    }
  }
}

static void
downHeap(int heapSize, int *topDocIDs, float *topScores) {
  int i = 1;
  // save top node
  int savDocID = topDocIDs[i];
  float savScore = topScores[i];
  int j = i << 1;            // find smaller child
  int k = j + 1;
  if (k <= heapSize && lessThan(topDocIDs[k], topScores[k], topDocIDs[j], topScores[j])) {
    j = k;
  }
  while (j <= heapSize && lessThan(topDocIDs[j], topScores[j], savDocID, savScore)) {
    // shift up child
    topDocIDs[i] = topDocIDs[j];
    topScores[i] = topScores[j];
    i = j;
    j = i << 1;
    k = j + 1;
    if (k <= heapSize && lessThan(topDocIDs[k], topScores[k], topDocIDs[j], topScores[j])) {
      j = k;
    }
  }
  // install saved node
  topDocIDs[i] = savDocID;
  topScores[i] = savScore;
}

static void
downHeapNoScores(int heapSize, int *topDocIDs) {
  int i = 1;
  // save top node
  int savDocID = topDocIDs[i];
  int j = i << 1;            // find smaller child
  int k = j + 1;
  if (k <= heapSize && topDocIDs[k] > topDocIDs[j]) {
    j = k;
  }
  while (j <= heapSize && topDocIDs[j] > savDocID) {
    // shift up child
    topDocIDs[i] = topDocIDs[j];
    i = j;
    j = i << 1;
    k = j + 1;
    if (k <= heapSize && topDocIDs[k] > topDocIDs[j]) {
      j = k;
    }
  }
  // install saved node
  topDocIDs[i] = savDocID;
}

static int
orFirstChunk(unsigned long *longBuffer,
             PostingsState *sub,
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
          nextBlock(longBuffer, sub);
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
      if (freq < 32) {
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
          nextBlock(longBuffer, sub);
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
orFirstChunkDeletes(unsigned long *longBuffer,
                    PostingsState *sub,
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
          nextBlock(longBuffer, sub);
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
        if (freq < 32) {
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
          nextBlock(longBuffer, sub);
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
orChunk(unsigned long *longBuffer,
        PostingsState *sub,
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
          nextBlock(longBuffer, sub);
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
      if (freq < 32) {
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
          nextBlock(longBuffer, sub);
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
orChunkDeletes(unsigned long *longBuffer,
               PostingsState *sub,
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
          nextBlock(longBuffer, sub);
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
        if (freq < 32) {
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
          nextBlock(longBuffer, sub);
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


extern "C" JNIEXPORT jint JNICALL
Java_org_apache_lucene_search_NativeSearch_searchSegmentBooleanQuery
  (JNIEnv *env,
   jclass cl,

   // PQ holding top hits so far, pre-filled with sentinel
   // values: 
   jintArray jtopDocIDs,
   jfloatArray jtopScores,

   // Current segment's maxDoc
   jint maxDoc,

   // Current segment's docBase
   jint docBase,

   // Current segment's liveDocs, or null:
   jbyteArray jliveDocBytes,

   // weightValue from each TermWeight:
   jfloatArray jtermWeights,

   // Norms for the field (all TermQuery must be against a single field):
   jbyteArray jnorms,

   // nocommit silly to pass this once for each segment:
   // Cache, mapping byte norm -> float
   jfloatArray jnormTable,

   // Coord factors from BQ:
   jfloatArray jcoordFactors,

   // If the term has only one docID in this segment (it was
   // "pulsed") then its set here, else -1:
   jintArray jsingletonDocIDs,

   // docFreq of each term
   jintArray jdocFreqs,

   // Offset in the .doc file where this term's docs+freqs begin:
   jlongArray jdocTermStartFPs,

   // Address in memory where .doc file is mapped:
   jlong docFileAddress)
{
  unsigned long __attribute__ ((aligned(16))) longBuffer[64]; 

  //printf("START search\n"); fflush(stdout);

  float *scores;
  unsigned int *coords;

  if (jtopScores == 0) {
    scores = 0;
    coords = 0;
  } else {
    scores = (float *) malloc(CHUNK * sizeof(float));
    coords = (unsigned int *) malloc(CHUNK * sizeof(int));
  }

  int *docIDs = (int *) malloc(CHUNK * sizeof(int));

  for(int i=0;i<CHUNK;i++) {
    docIDs[i] = -1;
  }

  int numScorers = env->GetArrayLength(jdocFreqs);

  int topN = env->GetArrayLength(jtopDocIDs) - 1;
  //printf("topN=%d\n", topN);

  PostingsState *subs = (PostingsState *) malloc(numScorers*sizeof(PostingsState));

  int *singletonDocIDs = env->GetIntArrayElements(jsingletonDocIDs, 0);
  long *docTermStartFPs = env->GetLongArrayElements(jdocTermStartFPs, 0);
  int *docFreqs = env->GetIntArrayElements(jdocFreqs, 0);
  float *termWeights = (float *) env->GetFloatArrayElements(jtermWeights, 0);
  float *coordFactors = (float *) env->GetFloatArrayElements(jcoordFactors, 0);

  unsigned char isCopy = 0;
  unsigned char *liveDocBytes;
  if (jliveDocBytes == 0) {
    liveDocBytes = 0;
  } else {
    liveDocBytes = (unsigned char *) env->GetPrimitiveArrayCritical(jliveDocBytes, &isCopy);
    //printf("liveDocs isCopy=%d\n", isCopy);fflush(stdout);
  }

  isCopy = 0;
  unsigned char* norms = (unsigned char *) env->GetPrimitiveArrayCritical(jnorms, &isCopy);
  //printf("norms isCopy=%d\n", isCopy);fflush(stdout);

  isCopy = 0;
  float *normTable = (float *) env->GetPrimitiveArrayCritical(jnormTable, &isCopy);
  //printf("normTable %lx isCopy=%d\n", normTable, isCopy);fflush(stdout);
  unsigned int *freq1 = (unsigned int *) malloc(sizeof(int));
  freq1[0] = 1;
  // Init scorers:
  for(int i=0;i<numScorers;i++) {
    PostingsState *sub = &(subs[i]);
    sub->docsOnly = false;
    sub->id = i;
    //printf("init scorers[%d] of %d\n", i, numScorers);

    if (singletonDocIDs[i] != -1) {
      //printf("  singleton: %d\n", singletonDocIDs[i]);
      sub->nextDocID = singletonDocIDs[i];
      sub->docsLeft = 0;
      sub->blockLastRead = 0;
      sub->blockEnd = 0;
      sub->docDeltas = 0;
      sub->freqs = freq1;
    } else {
      sub->docsLeft = docFreqs[i];
      if (scores != 0) {
        sub->docDeltas = (unsigned int *) malloc(2*BLOCK_SIZE*sizeof(int));
        // Locality seemed to help here:
        sub->freqs = sub->docDeltas + BLOCK_SIZE;
      } else {
        sub->docDeltas = (unsigned int *) malloc(BLOCK_SIZE*sizeof(int));
        sub->freqs = 0;
      }
      //printf("docFileAddress=%ld startFP=%ld\n", docFileAddress, docTermStartFPs[i]);fflush(stdout);
      sub->p = ((unsigned char *) docFileAddress) + docTermStartFPs[i];
      //printf("  not singleton\n");
      nextBlock(longBuffer, sub);
      sub->nextDocID = sub->docDeltas[0];
      //printf("docDeltas[0]=%d\n", sub->docDeltas[0]);
      sub->blockLastRead = 0;
    }
    //printf("init i=%d nextDocID=%d freq=%d blockEnd=%d singleton=%d\n", i, sub->nextDocID, sub->nextFreq, sub->blockEnd, singletonDocIDs[i]);fflush(stdout);
  }

  int docUpto = 0;

  // PQ holding top hits:
  int *topDocIDs = (int *) env->GetIntArrayElements(jtopDocIDs, 0);
  float *topScores;
  if (jtopScores != 0) {
    topScores = (float *) env->GetFloatArrayElements(jtopScores, 0);
  } else {
    topScores = 0;
  }
  unsigned int *filled = (unsigned int *) malloc(CHUNK * sizeof(int));
  int numFilled;
  int hitCount = 0;

  double **termScoreCache = (double **) malloc(numScorers*sizeof(double*));
  for(int i=0;i<numScorers;i++) {
    termScoreCache[i] = (double *) malloc(32*sizeof(double));
    for(int j=0;j<32;j++) {
      termScoreCache[i][j] = termWeights[i] * sqrt(j);
    }
  }

  while (docUpto < maxDoc) {
    register int endDoc = docUpto + CHUNK;
    //printf("cycle endDoc=%d dels=%lx\n", endDoc, liveDocBytes);fflush(stdout);

    if (liveDocBytes != 0) {
      // Collect first sub without if, since we know every
      // slot will be stale:
      numFilled = orFirstChunkDeletes(longBuffer, &subs[0], termScoreCache[0], termWeights[0], endDoc, filled, docIDs, scores, coords, liveDocBytes);
      for(int i=1;i<numScorers;i++) {
        numFilled = orChunkDeletes(longBuffer, &subs[i], termScoreCache[i], termWeights[i], endDoc, filled, numFilled, docIDs, scores, coords, liveDocBytes);
      }
    } else {
      // Collect first sub without if, since we know every
      // slot will be stale:
      numFilled = orFirstChunk(longBuffer, &subs[0], termScoreCache[0], termWeights[0], endDoc, filled, docIDs, scores, coords);
      for(int i=1;i<numScorers;i++) {
        numFilled = orChunk(longBuffer, &subs[i], termScoreCache[i], termWeights[i], endDoc, filled, numFilled, docIDs, scores, coords);
      }
    }

    hitCount += numFilled;
    
    if (topScores == 0) {
      for(int i=0;i<numFilled;i++) {
        int slot = filled[i];
        int docID = docBase + docIDs[slot];
        // TODO: we can stop after chunk once queue is full
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

  env->ReleasePrimitiveArrayCritical(jnorms, norms, JNI_ABORT);
  if (jliveDocBytes != 0) {
    env->ReleasePrimitiveArrayCritical(jliveDocBytes, liveDocBytes, JNI_ABORT);
  }
  env->ReleasePrimitiveArrayCritical(jnormTable, normTable, JNI_ABORT);

  env->ReleaseIntArrayElements(jsingletonDocIDs, singletonDocIDs, JNI_ABORT);
  env->ReleaseLongArrayElements(jdocTermStartFPs, docTermStartFPs, JNI_ABORT);
  env->ReleaseIntArrayElements(jdocFreqs, docFreqs, JNI_ABORT);
  env->ReleaseFloatArrayElements(jtermWeights, termWeights, JNI_ABORT);
  env->ReleaseFloatArrayElements(jcoordFactors, coordFactors, JNI_ABORT);

  env->ReleaseIntArrayElements(jtopDocIDs, topDocIDs, 0);
  if (jtopScores != 0) {
    env->ReleaseFloatArrayElements(jtopScores, topScores, 0);
  }

  for(int i=0;i<numScorers;i++) {
    free(termScoreCache[i]);
  }
  free(termScoreCache);
  free(filled);
  free(docIDs);
  if (scores != 0) {
    free(scores);
    free(coords);
  }
  free(freq1);
  for(int i=0;i<numScorers;i++) {
    PostingsState *sub = &(subs[i]);
    if (sub->docDeltas != 0) {
      free(sub->docDeltas);
    }
  }

  free(subs);

  return hitCount;
}


extern "C" JNIEXPORT jint JNICALL
Java_org_apache_lucene_search_NativeSearch_searchSegmentTermQuery
  (JNIEnv *env,
   jclass cl,

   // PQ holding top hits so far, pre-filled with sentinel
   // values: 
   jintArray jtopDocIDs,
   jfloatArray jtopScores,

   // Current segment's maxDoc
   jint maxDoc,

   // Current segment's docBase
   jint docBase,

   // Current segment's liveDocs, or null:
   jbyteArray jliveDocBytes,

   // weightValue from each TermWeight:
   jfloat termWeight,

   // Norms for the field (all TermQuery must be against a single field):
   jbyteArray jnorms,

   // nocommit silly to pass this once for each segment:
   // Cache, mapping byte norm -> float
   jfloatArray jnormTable,

   // If the term has only one docID in this segment (it was
   // "pulsed") then its set here, else -1:
   jint singletonDocID,

   // docFreq of each term
   jint docFreq,

   // Offset in the .doc file where this term's docs+freqs begin:
   jlong docTermStartFP,

   // Address in memory where .doc file is mapped:
   jlong docFileAddress)
{
  unsigned long __attribute__ ((aligned(16))) longBuffer[64]; 

  //printf("START search\n"); fflush(stdout);

  int topN = env->GetArrayLength(jtopDocIDs) - 1;
  //printf("topN=%d\n", topN);

  unsigned char isCopy = 0;
  unsigned char *liveDocBytes;
  if (jliveDocBytes == 0) {
    liveDocBytes = 0;
  } else {
    liveDocBytes = (unsigned char *) env->GetPrimitiveArrayCritical(jliveDocBytes, &isCopy);
    //printf("liveDocs isCopy=%d\n", isCopy);fflush(stdout);
  }

  isCopy = 0;
  unsigned char* norms = (unsigned char *) env->GetPrimitiveArrayCritical(jnorms, &isCopy);
  //printf("norms isCopy=%d\n", isCopy);fflush(stdout);

  isCopy = 0;
  float *normTable = (float *) env->GetPrimitiveArrayCritical(jnormTable, &isCopy);
  //printf("normTable %lx isCopy=%d\n", normTable, isCopy);fflush(stdout);

  register int totalHits = 0;
  // PQ holding top hits:
  int *topDocIDs = (int *) env->GetIntArrayElements(jtopDocIDs, 0);
  float *topScores;
  if (jtopScores == 0) {
    topScores = 0;
  } else {
    topScores = (float *) env->GetFloatArrayElements(jtopScores, 0);
  }

  if (singletonDocID != -1) {
    if (liveDocBytes == 0 || isSet(liveDocBytes, singletonDocID)) {
      topDocIDs[1] = singletonDocID;
      if (jtopScores != 0) {
        topScores[1] = termWeight * sqrt(1) * normTable[norms[singletonDocID]];
      }
      totalHits = 1;
    }
  } else {
    //printf("TQ: blocks\n");fflush(stdout);

#if 0
    // curiously this more straightforward impl is slower:
    PostingsState *sub = (PostingsState *) malloc(sizeof(PostingsState));
    sub->docsOnly = false;
    //printf("  set docsLeft=%d\n", docFreq);
    sub->docsLeft = docFreq;
    // Locality seemed to help here:
    if (jtopScores != 0) {
      sub->docDeltas = (unsigned int *) malloc(2*BLOCK_SIZE*sizeof(int));
      sub->freqs = sub->docDeltas + BLOCK_SIZE;
    } else {
      sub->docDeltas = (unsigned int *) malloc(BLOCK_SIZE*sizeof(int));
      sub->freqs = 0;
    }
    //printf("docFileAddress=%ld startFP=%ld\n", docFileAddress, docTermStartFPs[i]);fflush(stdout);
    sub->p = ((unsigned char *) docFileAddress) + docTermStartFP;
    int hitCount = 0;

    register double *termScoreCache = (double *) malloc(32*sizeof(double));
    for(int j=0;j<32;j++) {
      termScoreCache[j] = termWeight * sqrt(j);
    }

    register int nextDocID = 0;
    register unsigned int *docDeltas = sub->docDeltas;
    register unsigned int *freqs = sub->freqs;

    register int blockLastRead = sub->blockLastRead;
    register int blockEnd = sub->blockEnd;
  
    if (liveDocBytes != 0) {
      //printf("TQ: has liveDocs\n");fflush(stdout);
      if (topScores == 0) {
        while (sub->docsLeft != 0) {
          nextBlock(longBuffer, sub);
          register int limit = sub->blockEnd+1;
          for(register int i=0;i<limit;i++) {
            nextDocID += docDeltas[i];
            if (isSet(liveDocBytes, nextDocID)) {
              totalHits++;
              int docID = docBase + nextDocID;
              if (docID < topDocIDs[1]) {
                // Hit is competitive   
                topDocIDs[1] = docID;
                downHeapNoScores(topN, topDocIDs);
              }
            }
          }
        }
      } else {
        while (sub->docsLeft != 0) {
          nextBlock(longBuffer, sub);
          register int limit = sub->blockEnd+1;
          //printf("limit=%d\n", limit);
          for(register int i=0;i<limit;i++) {
            nextDocID += docDeltas[i];
            if (isSet(liveDocBytes, nextDocID)) {
              totalHits++;

              float score;
              int freq = freqs[i];

              if (freq < 32) {
                score = termScoreCache[freq];
              } else {
                score = sqrt(freq) * termWeight;
              }

              score *= normTable[norms[nextDocID]];

              int docID = docBase + nextDocID;

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
    } else {
      //printf("TQ: no liveDocs\n");fflush(stdout);

      if (topScores == 0) {
        //printf("TQ: has topScores\n");fflush(stdout);
        while (sub->docsLeft != 0) {
          nextBlock(longBuffer, sub);
          register int limit = sub->blockEnd+1;
          for(register int i=0;i<limit;i++) {
            nextDocID += docDeltas[i];

            int docID = docBase + nextDocID;

            // TODO: this is silly: only first topN docs are
            // competitive, after that we should break
            if (docID < topDocIDs[1]) {
              // Hit is competitive   
              topDocIDs[1] = docID;
              downHeapNoScores(topN, topDocIDs);
            }
          }
        }
      } else {
        while (sub->docsLeft != 0) {
          nextBlock(longBuffer, sub);
          register int limit = sub->blockEnd+1;
          for(register int i=0;i<limit;i++) {
            nextDocID += docDeltas[i];

            float score;
            int freq = freqs[i];

            if (freq < 32) {
              score = termScoreCache[freq];
            } else {
              score = sqrt(freq) * termWeight;
            }

            score *= normTable[norms[nextDocID]];

            int docID = docBase + nextDocID;

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

      // No deletions, so totalHits == docFreq
      totalHits = docFreq;
    }

#else

    PostingsState *sub = (PostingsState *) malloc(sizeof(PostingsState));
    sub->docsOnly = false;
    sub->docsLeft = docFreq;
    // Locality seemed to help here:
    if (jtopScores != 0) {
      sub->docDeltas = (unsigned int *) malloc(2*BLOCK_SIZE*sizeof(int));
      sub->freqs = sub->docDeltas + BLOCK_SIZE;
    } else {
      sub->docDeltas = (unsigned int *) malloc(BLOCK_SIZE*sizeof(int));
      sub->freqs = 0;
    }
    //printf("docFileAddress=%ld startFP=%ld\n", docFileAddress, docTermStartFPs[i]);fflush(stdout);
    sub->p = ((unsigned char *) docFileAddress) + docTermStartFP;
    //printf("  not singleton\n");
    nextBlock(longBuffer, sub);
    sub->nextDocID = sub->docDeltas[0];
    //printf("docDeltas[0]=%d\n", sub->docDeltas[0]);
    sub->blockLastRead = 0;

    register double *termScoreCache = (double *) malloc(32*sizeof(double));
    for(int j=0;j<32;j++) {
      termScoreCache[j] = termWeight * sqrt(j);
    }

    register int nextDocID = sub->nextDocID;
    register unsigned int *docDeltas = sub->docDeltas;
    register unsigned int *freqs = sub->freqs;

    register int blockLastRead = sub->blockLastRead;
    register int blockEnd = sub->blockEnd;
  
    if (liveDocBytes != 0) {
      if (topScores == 0) {
        while (true) {

          if (isSet(liveDocBytes, nextDocID)) {
            totalHits++;

            int docID = docBase + nextDocID;

            if (docID < topDocIDs[1]) {
              // Hit is competitive   
              topDocIDs[1] = docID;
              downHeapNoScores(topN, topDocIDs);
            }
          }

          // Inlined nextDoc:
          if (blockLastRead == blockEnd) {
            if (sub->docsLeft == 0) {
              break;
            } else {
              nextBlock(longBuffer, sub);
              blockLastRead = -1;
              blockEnd = sub->blockEnd;
            }
          }

          nextDocID += docDeltas[++blockLastRead];
        }
      } else {
        while (true) {
          if (isSet(liveDocBytes, nextDocID)) {
            totalHits++;

            float score;
            int freq = freqs[blockLastRead];

            if (freq < 32) {
              score = termScoreCache[freq];
            } else {
              score = sqrt(freq) * termWeight;
            }

            score *= normTable[norms[nextDocID]];

            int docID = docBase + nextDocID;

            if (score > topScores[1] || (score == topScores[1] && docID < topDocIDs[1])) {
              // Hit is competitive   
              topDocIDs[1] = docID;
              topScores[1] = score;

              downHeap(topN, topDocIDs, topScores);

              //printf("    **\n");fflush(stdout);
            }
          }

          // Inlined nextDoc:
          if (blockLastRead == blockEnd) {
            if (sub->docsLeft == 0) {
              break;
            } else {
              nextBlock(longBuffer, sub);
              blockLastRead = -1;
              blockEnd = sub->blockEnd;
            }
          }

          nextDocID += docDeltas[++blockLastRead];
        }
      }
    } else {

      if (topScores == 0) {
        while (true) {

          int docID = docBase + nextDocID;

          // TODO: this is silly: only first topN docs are
          // competitive, after that we should break
          if (docID < topDocIDs[1]) {
            // Hit is competitive   
            topDocIDs[1] = docID;

            downHeapNoScores(topN, topDocIDs);
          }

          // Inlined nextDoc:
          if (blockLastRead == blockEnd) {
            if (sub->docsLeft == 0) {
              break;
            } else {
              nextBlock(longBuffer, sub);
              blockLastRead = -1;
              blockEnd = sub->blockEnd;
            }
          }

          nextDocID += docDeltas[++blockLastRead];
        }
      } else {

        while (true) {

          float score;
          int freq = freqs[blockLastRead];

          if (freq < 32) {
            score = termScoreCache[freq];
          } else {
            score = sqrt(freq) * termWeight;
          }

          score *= normTable[norms[nextDocID]];

          int docID = docBase + nextDocID;

          if (score > topScores[1] || (score == topScores[1] && docID < topDocIDs[1])) {
            // Hit is competitive   
            topDocIDs[1] = docID;
            topScores[1] = score;

            downHeap(topN, topDocIDs, topScores);
            //printf("    **\n");fflush(stdout);
          }

          // Inlined nextDoc:
          if (blockLastRead == blockEnd) {
            if (sub->docsLeft == 0) {
              break;
            } else {
              nextBlock(longBuffer, sub);
              blockLastRead = -1;
              blockEnd = sub->blockEnd;
            }
          }

          nextDocID += docDeltas[++blockLastRead];
        }
      }

      totalHits = docFreq;
    }
#endif

    free(termScoreCache);
    if (sub->docDeltas != 0) {
      free(sub->docDeltas);
    }

    free(sub);
  }

  env->ReleasePrimitiveArrayCritical(jnorms, norms, JNI_ABORT);
  if (jliveDocBytes != 0) {
    env->ReleasePrimitiveArrayCritical(jliveDocBytes, liveDocBytes, JNI_ABORT);
  }
  env->ReleasePrimitiveArrayCritical(jnormTable, normTable, JNI_ABORT);

  env->ReleaseIntArrayElements(jtopDocIDs, topDocIDs, 0);
  if (jtopScores != 0) {
    env->ReleaseFloatArrayElements(jtopScores, topScores, 0);
  }

  return totalHits;
}

extern "C" JNIEXPORT void JNICALL
Java_org_apache_lucene_search_NativeSearch_fillMultiTermFilter
  (JNIEnv *env,
   jclass cl,
   jlongArray jbits,
   jbyteArray jliveDocsBytes,
   jlong address,
   jlongArray jtermStats,
   jboolean docsOnly) {

  unsigned long __attribute__ ((aligned(16))) longBuffer[64]; 
  unsigned char isCopy = 0;

  unsigned char *liveDocsBytes;
  if (jliveDocsBytes == 0) {
    liveDocsBytes = 0;
  } else {
    liveDocsBytes = (unsigned char *) env->GetPrimitiveArrayCritical(jliveDocsBytes, &isCopy);
  }

  isCopy = 0;
  long *bits = (long *) env->GetPrimitiveArrayCritical(jbits, &isCopy);

  long *termStats = (long *) env->GetPrimitiveArrayCritical(jtermStats, 0);

  PostingsState *sub = (PostingsState *) malloc(sizeof(PostingsState));
  sub->docsOnly = (bool) docsOnly;
  sub->docDeltas = (unsigned int *) malloc(BLOCK_SIZE * sizeof(int));
  sub->freqs = 0;

  register unsigned int *docDeltas = sub->docDeltas;

  int numTerms = env->GetArrayLength(jtermStats);
  //printf("numTerms=%d\n", numTerms);
  int i = 0;
  while (i < numTerms) {
    sub->docsLeft = (int) termStats[i++];
    sub->p = (unsigned char *) (address + termStats[i++]);
    nextBlock(longBuffer, sub);
    int nextDocID = 0;

    //printf("do term %d docFreq=%d\n", i, sub->docsLeft);fflush(stdout);

    while (true) {
      register int limit = sub->blockEnd+1;
      //printf("  limit %d\n", limit);fflush(stdout);
      if (liveDocsBytes == 0) {
        for(int j=0;j<limit;j++) {
          nextDocID += docDeltas[j];
          setLongBit(bits, nextDocID);
        }
      } else {
        for(int j=0;j<limit;j++) {
          nextDocID += docDeltas[j];
          setLongBit(bits, nextDocID);
        }
      }
      if (sub->docsLeft == 0) {
        break;
      } else {
        nextBlock(longBuffer, sub);
      }
    }
  }

  free(sub->docDeltas);
  free(sub);
  
  if (jliveDocsBytes != 0) {
    env->ReleasePrimitiveArrayCritical(jliveDocsBytes, liveDocsBytes, JNI_ABORT);
  }
  env->ReleasePrimitiveArrayCritical(jtermStats, termStats, JNI_ABORT);
  env->ReleasePrimitiveArrayCritical(jbits, bits, JNI_ABORT);
}

