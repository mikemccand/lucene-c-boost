package org.apache.lucene.store;

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

import java.io.File;
import java.io.EOFException;
import java.io.RandomAccessFile;
import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.reflect.*;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import sun.misc.Unsafe;

public class NativeMMapDirectory extends FSDirectory {

  static {
    System.loadLibrary("NativeMMapDirectory");
  }
  
  private final static native long map(int fd, long fileLength);
  private final static native void unmap(long address, long fileLength);

  public NativeMMapDirectory(File path) throws IOException {
    this(path, null);
  }

  public NativeMMapDirectory(File path, LockFactory lockFactory) throws IOException {
    super(path, lockFactory);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    File file = new File(getDirectory(), name);
    //try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
    return new NativeMMapIndexInput("NativeMMapIndexInput(path=\"" + file.toString() + "\")", new RandomAccessFile(file, "r"));
  }

  @Override
  public IndexInputSlicer createSlicer(String name, IOContext context) throws IOException {
    final NativeMMapIndexInput full = (NativeMMapIndexInput) openInput(name, context);

    return new IndexInputSlicer() {
      @Override
      public IndexInput openSlice(String sliceDescription, long offset, long length) throws IOException {
        ensureOpen();
        return new NativeMMapIndexInput(full.toString() + "[slice=" + sliceDescription + "]", full.address + offset, length, full.address + offset);
      }
      
      @Override
      public IndexInput openFullSlice() throws IOException {
        ensureOpen();
        return full.clone();
      }

      @Override
      public void close() throws IOException {
        full.close();
      }
    };
  }

  int getFileDes(FileDescriptor fd) {
    try {
      Class<?> x = Class.forName("java.io.FileDescriptor");
      Field f = x.getDeclaredField("fd");
      f.setAccessible(true);
      return f.getInt(fd);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static final Unsafe unsafe;
  
  static {
    try {
      Field f = Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      unsafe = (Unsafe) f.get(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  static final long arrayBaseOffset = (long) unsafe.arrayBaseOffset(byte[].class);

  private final class NativeMMapIndexInput extends IndexInput {

    private final long address;
    private final long length;
    private final RandomAccessFile raf;

    private long pos;

    NativeMMapIndexInput(String resourceDescription, RandomAccessFile raf) throws IOException {
      super(resourceDescription);
      this.raf = raf;
      length = raf.length();
      if (length == 0) {
        // mmap gets angry if you map 0 length file:
        address = 0;
      } else {
        int fd = getFileDes(raf.getFD());
        address = map(fd, length);
        //System.out.println("map: " + resourceDescription + " fd=" + fd + " -> address=" + address + " length=" + length);
      }
      pos = address;
    }

    NativeMMapIndexInput(String resourceDescription, long address, long length, long pos) {
      super(resourceDescription);
      raf = null;
      this.address = address;
      this.length = length;
      this.pos = pos;
    }

    @Override
    public long getFilePointer() {
      return pos - address;
    }

    @Override
    public void seek(long pos) {
      this.pos = address + pos;
    }

    @Override
    public long length() {
      return length;
    }

    @Override
    public void close() throws IOException {
      // nocommit guard against double close
      if (raf != null) {
        unmap(address, length);
        raf.close();
      }
    }

    @Override
    public byte readByte() throws IOException {
      // Necessary for reading segments_N while IW is committing:
      if (pos >= address + length) {
        throw new EOFException();
      }
      return unsafe.getByte(pos++);
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      // Necessary for reading segments_N while IW is committing:
      if (pos + len > address + length) {
        throw new EOFException();
      }
      unsafe.copyMemory(null, pos, b, arrayBaseOffset + offset, len);
      pos += len;
    }

    @Override
    public NativeMMapIndexInput clone() {
      return new NativeMMapIndexInput(toString(), address, length, pos);
    }
  }
}
