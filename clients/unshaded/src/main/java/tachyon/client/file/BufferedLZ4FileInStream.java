/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.file;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.Checksum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;

import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.util.SafeUtils;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

/**
 * @author Daniel 2015-10-20
 */
public class BufferedLZ4FileInStream extends FilterInputStream {
  private static final byte[] MAGIC = new byte[] {'L', 'Z', '4', 'B', 'l', 'o', 'c', 'k'};
  private static final int MAGIC_LENGTH = MAGIC.length;

  public static final int HEADER_LENGTH = MAGIC_LENGTH // magic bytes
      + 1 // token
      + 4 // compressed length
      + 4 // decompressed length
      + 4; // mChecksum

  private static final int COMPRESSION_LEVEL_BASE = 10;
  private static final int MIN_BLOCK_SIZE = 64;
  private static final int MAX_BLOCK_SIZE = 1 << (COMPRESSION_LEVEL_BASE + 0x0F);

  private static final int COMPRESSION_METHOD_RAW = 0x10;
  private static final int COMPRESSION_METHOD_LZ4 = 0x20;

  private static final int DEFAULT_SEED = 0x9747b28c;
  /** Logger for this class */
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final LZ4FastDecompressor mDecompressor;
  private final Checksum mChecksum;
  private byte[] mBuffer;
  private byte[] mCompressedBuffer;
  private byte[] mTempHeaderBuffer;
  private byte[] mTempCompressBuffer;
  private int mOriginalLen;
  private int mOffset;
  private boolean mFinished;
  private boolean mNeedNewBlock;
  private long mPos;

  /**
   * Create a new {@link InputStream}.
   *
   * @param in the {@link InputStream} to poll
   * @param decompressor the {@link LZ4FastDecompressor mDecompressor} instance to use
   * @param checksum the {@link Checksum} instance to use, must be equivalent to the instance which
   *        has been used to write the stream
   */
  public BufferedLZ4FileInStream(InputStream in, LZ4FastDecompressor decompressor,
      Checksum checksum) {
    super(in);
    mDecompressor = decompressor;
    mChecksum = checksum;
    mBuffer = new byte[0];
    mCompressedBuffer = new byte[HEADER_LENGTH];
    mOffset = mOriginalLen = 0;
    mFinished = false;
    mNeedNewBlock = false;
    mTempHeaderBuffer = new byte[0];
    mTempCompressBuffer = new byte[0];
    mPos = 0;
  }

  /**
   * Create a new instance using {@link XXHash32} for checksuming.
   *
   * @see #BufferedLZ4FileInStream(InputStream, LZ4FastDecompressor, Checksum)
   * @see StreamingXXHash32#asChecksum()
   */
  public BufferedLZ4FileInStream(InputStream in, LZ4FastDecompressor mDecompressor) {
    this(in, mDecompressor,
        XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum());
  }

  /**
   * Create a new instance which uses the fastest {@link LZ4FastDecompressor} available.
   *
   * @see LZ4Factory#fastestInstance()
   * @see #BufferedLZ4FileInStream(InputStream, LZ4FastDecompressor)
   */
  public BufferedLZ4FileInStream(InputStream in) {
    this(in, LZ4Factory.fastestInstance().fastDecompressor());
  }

  public BufferedLZ4FileInStream reInitialize(InputStream inputStream) {
    in = inputStream;
    mBuffer = new byte[0];
    mCompressedBuffer = new byte[HEADER_LENGTH];
    mOffset = mOriginalLen = 0;
    mFinished = false;
    mNeedNewBlock = false;
    return this;
  }

  @Override
  public int available() throws IOException {
    return mOriginalLen - mOffset;
  }

  @Override
  public int read() throws IOException {
    if (mFinished) {
      return -1;
    }
    if (mOffset == mOriginalLen) {
      refill();
    }
    if (mNeedNewBlock) {
      return -2;
    }
    if (mFinished) {
      return -1;
    }
    return mBuffer[mOffset++] & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    SafeUtils.checkRange(b, off, len);
    if (mFinished) {
      return -1;
    }
    if (mOffset == mOriginalLen) {
      refill();
    }
    if (mNeedNewBlock) {
      return -1;
    }
    if (mFinished) {
      return -1;
    }
    len = Math.min(len, mOriginalLen - mOffset);
    System.arraycopy(mBuffer, mOffset, b, off, len);
    mOffset += len;
    return len;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public long skip(long n) throws IOException {
    throw new IOException("skip not supported");
  }

  private void refill() throws IOException {
    if (!readFully(mCompressedBuffer, HEADER_LENGTH)) {
      mNeedNewBlock = true;
      return;
    }
    mTempHeaderBuffer = new byte[HEADER_LENGTH];
    System.arraycopy(mCompressedBuffer,0,mTempHeaderBuffer,0,HEADER_LENGTH);
    for (int i = 0; i < MAGIC_LENGTH; ++i) {
      if (mCompressedBuffer[i] != MAGIC[i]) {
        throw new IOException(
            "Stream is corrupted at " + i + " : " + mCompressedBuffer[i] + " - " + MAGIC[i]);
      }
    }
    final int token = mCompressedBuffer[MAGIC_LENGTH] & 0xFF;
    final int compressionMethod = token & 0xF0;
    final int compressionLevel = COMPRESSION_LEVEL_BASE + (token & 0x0F);
    if (compressionMethod != COMPRESSION_METHOD_RAW
        && compressionMethod != COMPRESSION_METHOD_LZ4) {
      throw new IOException("Stream is corrupted: invalid compressionMethod");
    }
    final int compressedLen = SafeUtils.readIntLE(mCompressedBuffer, MAGIC_LENGTH + 1);
    mOriginalLen = SafeUtils.readIntLE(mCompressedBuffer, MAGIC_LENGTH + 5);
    final int check = SafeUtils.readIntLE(mCompressedBuffer, MAGIC_LENGTH + 9);
    assert HEADER_LENGTH == MAGIC_LENGTH + 13;
    if (mOriginalLen > 1 << compressionLevel || mOriginalLen < 0 || compressedLen < 0
        || (mOriginalLen == 0 && compressedLen != 0) || (mOriginalLen != 0 && compressedLen == 0)
        || (compressionMethod == COMPRESSION_METHOD_RAW && mOriginalLen != compressedLen)) {
      throw new IOException("Stream is corrupted: invalid length");
    }
    if (mOriginalLen == 0 && compressedLen == 0) {
      if (check != 0) {
        throw new IOException("Stream is corrupted: invalid checksum");
      }
      mFinished = true;
      return;
    }
    if (mBuffer.length < mOriginalLen) {
      mBuffer = new byte[Math.max(mOriginalLen, mBuffer.length * 3 / 2)];
    }
    switch (compressionMethod) {
      case COMPRESSION_METHOD_RAW:
        if (!readFully(mBuffer, mOriginalLen)) {
          LOG.info("raw compression not read fully");
          mNeedNewBlock = true;
          return;
        }
        break;
      case COMPRESSION_METHOD_LZ4:
        if (mCompressedBuffer.length < mOriginalLen) {
          mCompressedBuffer = new byte[Math.max(compressedLen, mCompressedBuffer.length * 3 / 2)];
        }
        if (!readFully(mCompressedBuffer, compressedLen)) {
          LOG.info("lz4 compression not read fully");
          mNeedNewBlock = true;
          return;
        }
        try {
          final int compressedLen2 =
              mDecompressor.decompress(mCompressedBuffer, 0, mBuffer, 0, mOriginalLen);
          if (compressedLen != compressedLen2) {
            throw new IOException("Stream is corrupted: invalid decompression");
          }
        } catch (LZ4Exception e) {
          throw new IOException("Stream is corrupted: ", e);
        }
        break;
      default:
        throw new AssertionError();
    }
    mChecksum.reset();
    mChecksum.update(mBuffer, 0, mOriginalLen);
    if ((int) mChecksum.getValue() != check) {
      throw new IOException("Stream is corrupted: invalid decompression checksum: "
          + mChecksum.getValue() + " -- " + check);
    }
    mOffset = 0;
    mTempHeaderBuffer = new byte[0];
  }

  private boolean readFully(byte[] b, int len) throws IOException {
    int read = 0;
    if (len == HEADER_LENGTH) {
      if (mTempHeaderBuffer.length > 0) {
        System.arraycopy(mTempHeaderBuffer, 0, b, 0, mTempHeaderBuffer.length);
        read += mTempHeaderBuffer.length;
        mTempHeaderBuffer = new byte[0];
      }
    } else {
      if (mTempCompressBuffer.length > 0) {
        if (mTempCompressBuffer.length > len) {
          System.arraycopy(mTempCompressBuffer, 0, b, 0, len);
          byte[] newTempBuffer = new byte[mTempCompressBuffer.length - len];
          System.arraycopy(mTempCompressBuffer, len, newTempBuffer, 0,
              mTempCompressBuffer.length - len);
          mTempCompressBuffer = newTempBuffer;
          return true;
        } else {
          System.arraycopy(mTempCompressBuffer, 0, b, 0, mTempCompressBuffer.length);
          read += mTempCompressBuffer.length;
          mTempCompressBuffer = new byte[0];
        }
      }
    }
    while (read < len) {
      final int r = in.read(b, read, len - read);
      if (r < 0) {
        break;
      }
      read += r;
      mPos += r;
    }
    if (len != read) {
      if (len == HEADER_LENGTH) {
        mTempHeaderBuffer = new byte[read];
        System.arraycopy(b, 0, mTempHeaderBuffer, 0, read);
      } else {
        mTempCompressBuffer = new byte[read];
        System.arraycopy(b, 0, mTempCompressBuffer, 0, read);
      }
    }
    return len == read;
  }

  public long getPos() {
    return mPos;
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @SuppressWarnings("sync-override")
  @Override
  public void mark(int readlimit) {
    // unsupported
  }

  @SuppressWarnings("sync-override")
  @Override
  public void reset() throws IOException {
    throw new IOException("mark/reset not supported");
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(in=" + in + ", mDecompressor=" + mDecompressor
        + ", mChecksum=" + mChecksum + ")";
  }

}
