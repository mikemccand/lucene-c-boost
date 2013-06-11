#! /usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import StringIO

"""Code generation for bulk operations"""

MAX_SPECIALIZED_BITS_PER_VALUE = 24;
PACKED_64_SINGLE_BLOCK_BPV = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 16, 21, 32]
OUTPUT_FILE = "src/c/org/apache/lucene/search/NativeSearch.cpp"

def is_power_of_two(n):
  return n & (n - 1) == 0

def casts(typ):
  cast_start = "(%s) (" %typ
  cast_end = ")"
  return cast_start, cast_end

def hexNoLSuffix(n):
  # On 32 bit Python values > (1 << 31)-1 will have L appended by hex function:
  s = hex(n)
  if s.endswith('L'):
    s = s[:-1]
  return s

def masks(bits):
  if bits == 64:
    return "", ""
  return "(", " & %sL)" %(hexNoLSuffix((1 << bits) - 1))

def get_type(bits):
  if bits == 8:
    return "byte"
  elif bits == 16:
    return "short"
  elif bits == 32:
    return "int"
  elif bits == 64:
    return "long"
  else:
    assert False

def block_value_count(bpv, bits=64):
  blocks = bpv
  values = blocks * bits / bpv
  iters = 2
  while blocks % 2 == 0 and values % 2 == 0:
    blocks /= 2
    values /= 2
    iters *= 2
  assert values * bpv == bits * blocks, "%d values, %d blocks, %d bits per value" %(values, blocks, bpv)
  return blocks, values, iters

def p64_decode(bpv, f):
  mask = (1 << bpv) - 1
  
  blocks, values, iters = block_value_count(bpv)
  cast_start, cast_end = casts('int')

  f.write("static void decode%d(unsigned long *blocks, unsigned int *values) {\n" % bpv)
  f.write("  int blocksOffset = 0;\n")
  f.write("  int valuesOffset = 0;\n")
  #f.write('  printf("decode %s\\n");\n' % bpv);
  f.write("  for (int i = 0; i < %d; ++i) {\n" % iters)

  if is_power_of_two(bpv):
    f.write("    unsigned long block = blocks[blocksOffset++];\n")
    f.write("    block = __bswap_64(block);\n")
    f.write("    for (int shift = %d; shift >= 0; shift -= %d) {\n" %(64 - bpv, bpv))
    f.write("      values[valuesOffset++] = %s(block >> shift) & %d%s;\n" %(cast_start, mask, cast_end))
    f.write("    }\n") 
  else:
    for i in xrange(0, values):
      block_offset = i * bpv / 64
      bit_offset = (i * bpv) % 64
      if bit_offset == 0:
        # start of block
        f.write("    unsigned long block%d = blocks[blocksOffset++];\n" %block_offset);
        f.write("    block%d = __bswap_64(block%d);\n" % (block_offset, block_offset))
        f.write("    values[valuesOffset++] = %sblock%d >> %d%s;\n" %(cast_start, block_offset, 64 - bpv, cast_end))
      elif bit_offset + bpv == 64:
        # end of block
        f.write("    values[valuesOffset++] = %sblock%d & %dL%s;\n" %(cast_start, block_offset, mask, cast_end))
      elif bit_offset + bpv < 64:
        # middle of block
        f.write("    values[valuesOffset++] = %s(block%d >> %d) & %dL%s;\n" %(cast_start, block_offset, 64 - bit_offset - bpv, mask, cast_end))
      else:
        # value spans across 2 blocks
        mask1 = (1 << (64 - bit_offset)) -1
        shift1 = bit_offset + bpv - 64
        shift2 = 64 - shift1
        f.write("    unsigned long block%d = blocks[blocksOffset++];\n" %(block_offset + 1));
        f.write("    block%d = __bswap_64(block%d);\n" % (block_offset+1, block_offset+1))
        f.write("    values[valuesOffset++] = %s((block%d & %dL) << %d) | (block%d >> %d)%s;\n" %(cast_start, block_offset, mask1, shift1, block_offset + 1, shift2, cast_end))
  f.write("  }\n")
  f.write("}\n")


if __name__ == '__main__':
  f = StringIO.StringIO()
  f.write('// BEGIN AUTOGEN CODE (gen_Packed.py)\n')
  
  for bpv in xrange(1, 32):
    if bpv in (1, 2, 4):
      # Handled by decodeSingleBlockN
      continue
    f.write('\n')
    p64_decode(bpv, f)

  f.write('''

static void readPackedBlock(unsigned long *longBuffer, PostingsState *sub, unsigned int *dest) {
  unsigned char bitsPerValue = readByte(sub);
  //printf("\\nreadPackedBlock bpv=%d\\n", bitsPerValue);
  if (bitsPerValue == 0) {
    // All values equal
    unsigned int v = readVInt(sub);
    for(int i=0;i<BLOCK_SIZE;i++) {
      dest[i] = v;
    }
  } else {
    int numBytes = bitsPerValue*16;
    //printf("\\n  %d bytes @ p=%d\\n", numBytes, (int) (sub->p - globalAddress));
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
''')
          
  for bpv in xrange(1, 32):
    f.write('      case %d:\n' % bpv)
    if bpv in (1,2,4):
      f.write('        decodeSingleBlock%d(longBuffer, dest);\n' % bpv)
    else:
      f.write('        decode%d(longBuffer, dest);\n' % bpv)
    f.write('        break;\n')
          
  f.write('''
    }
  }
}
''')  

  f.write('// END AUTOGEN CODE (gen_Packed.py)\n')

  s = f.getvalue()

  s2 = open(OUTPUT_FILE, 'rb').read()
  i = s2.find('// BEGIN AUTOGEN CODE (gen_Packed.py)\n')
  if i == -1:
    raise RuntimeError('cannot find BEGIN AUTOGEN comment')
  j = s2.find('// END AUTOGEN CODE (gen_Packed.py)\n')
  if j == -1:
    raise RuntimeError('cannot find END AUTOGEN comment')

  s2 = s2[:i] + s + s2[j+36:]

  open(OUTPUT_FILE, 'wb').write(s2)
  
