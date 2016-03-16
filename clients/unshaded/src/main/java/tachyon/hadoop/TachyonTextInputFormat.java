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

package tachyon.hadoop;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

import tachyon.Constants;
import tachyon.client.ClientContext;

/**
 * @author Daniel 2016-03-09
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TachyonTextInputFormat extends FileInputFormat<LongWritable, Text>
    implements JobConfigurable {
  /** Logger for this class */
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private boolean mCompression = false;
  private CompressionCodecFactory mCompressionCodecs = null;

  public void configure(JobConf conf) {
    mCompression = ClientContext.getConf().getBoolean(Constants.USER_FILE_COMPRESSION_ENABLED);
    mCompressionCodecs = new CompressionCodecFactory(conf);
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path file) {
    final CompressionCodec codec = mCompressionCodecs.getCodec(file);
    if (mCompression) {
      LOG.info("compression block, not splittable!");
      return false;
    } else {
      if (null == codec) {
        return true;
      }
      return codec instanceof SplittableCompressionCodec;
    }
  }

  public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job,
      Reporter reporter) throws IOException {

    reporter.setStatus(genericSplit.toString());
    String delimiter = job.get("textinputformat.record.delimiter");
    byte[] recordDelimiterBytes = null;
    if (null != delimiter) {
      recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
    }
    return new LineRecordReader(job, (FileSplit) genericSplit, recordDelimiterBytes);
  }
}
