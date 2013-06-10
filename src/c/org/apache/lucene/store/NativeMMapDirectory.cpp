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

#include <sys/mman.h>
#include <errno.h>
#include <jni.h>

extern "C" JNIEXPORT jlong JNICALL
Java_org_apache_lucene_store_NativeMMapDirectory_map(JNIEnv *env,
                                                     jclass cl,
                                                     jint fd, jlong fileLength) {
  long address = (long) mmap(0, fileLength, PROT_READ, MAP_SHARED, fd, 0);
  if (address == -1) {
    jclass exClass = env->FindClass("java/io/IOException");
    char buf[64];
    sprintf(buf, "errno=%d", errno);
    return env->ThrowNew(exClass, buf);
  }
  return address;
}

extern "C" JNIEXPORT jlong JNICALL
Java_org_apache_lucene_store_NativeMMapDirectory_unmap(JNIEnv *env,
                                                       jclass cl,
                                                       jlong address, jlong fileLength) {
  munmap((void *) address, fileLength);
}
