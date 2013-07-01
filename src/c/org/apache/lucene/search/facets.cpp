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

#include <string.h>
#include <stdio.h>
#include "common.h"

int
nextSetBit(unsigned long *bits, unsigned int numWords, int index) {
  //printf("nextSetBit index=%d\n", index);fflush(stdout);
  unsigned int i = index >> 6;
  unsigned int subIndex = index & 0x3f;      // index within the word
  long word = bits[i] >> subIndex;
  if (word != 0) {
    //printf("  ret %d\n", ((i << 6) + subIndex + ffsl(word) - 1));fflush(stdout);
    return (i << 6) + subIndex + ffsl(word) - 1;
  }

  while(++i < numWords) {
    word = bits[i];
    if (word != 0) {
      //printf("  ret2 %d\n", (i << 6) + ffsl(word) - 1);fflush(stdout);
      return (i << 6) + ffsl(word) - 1;
    }
  }

  return NO_MORE_DOCS;
}

void
accum(unsigned int upto, unsigned int endUpto, unsigned int *facetCounts, unsigned char *facetBytes) {
  unsigned int ord = 0;
  unsigned int prev = 0;
  //printf("    accum address=%d len=%d\n", upto, endUpto-upto); fflush(stdout);
  while (upto < endUpto) {
    unsigned char b = facetBytes[upto++];
    //printf("      byte=%x\n", b);
    if ((b & 0x80) == 0) {
      prev = ord = ((ord << 7) | b) + prev;
      //printf("      incr ord=%d\n", ord); fflush(stdout);
      facetCounts[ord]++;
      ord = 0;
    } else {
      ord = (ord << 7) | (b & 0x7F);
    }
  }
}

unsigned int
decode26(unsigned long *packed, unsigned int index) {
  // The abstract index in a bit stream
  long majorBitPos = (long) index * 26;
  // The index in the backing long-array
  int elementPos = (int) (majorBitPos >> 6);
  // The number of value-bits in the second long
  long endBits = (majorBitPos & 63) + 26 - 64;

  //printf("decode26: index=%d elementPos=%d\n", index, elementPos);fflush(stdout);

  if (endBits <= 0) { // Single block
    //printf("  single block endBits=%d: %d\n", endBits, (packed[elementPos] >> -endBits) & 67108863);fflush(stdout);
    return (packed[elementPos] >> -endBits) & 67108863;
  }

  // Two blocks
  int result = ((packed[elementPos] << endBits) | (packed[elementPos+1] >> (64 - endBits))) & 67108863;
  //printf("  two blocks: %d\n", result);fflush(stdout);
  return result;
}

void
countFacets(unsigned long *bits, unsigned int maxDoc, unsigned int *facetCounts, unsigned long *dvDocToAddress, unsigned char *dvFacetBytes) {
  unsigned int numWords = (maxDoc + 63)/64;
  int doc = nextSetBit(bits, numWords, doc);
  //printf("\ncountFacets maxDoc=%d\n", maxDoc);
  while (doc < maxDoc) {
    //printf("  cycle doc=%d\n", doc);fflush(stdout);
    accum(decode26(dvDocToAddress, doc),
          decode26(dvDocToAddress, doc+1),
          facetCounts,
          dvFacetBytes);
    if (doc == maxDoc-1) {
      break;
    }
    doc = nextSetBit(bits, numWords, doc+1);
  }
}
