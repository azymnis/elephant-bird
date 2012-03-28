package com.twitter.elephantbird.cascading2.scheme;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import com.twitter.elephantbird.mapred.input.DeprecatedInputFormatWrapper;
import com.twitter.elephantbird.mapred.output.DeprecatedLzoProtobufBlockOutputFormat;
import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.Tap;

import java.io.IOException;

/**
 * Scheme for Protobuf block and B64 encoded files.
 *
 * @author Avi Bryant, Ning Liang
 */
public class LzoProtobufScheme extends
  LzoBinaryScheme<ProtobufWritable<?>> {

  private static final long serialVersionUID = -5011096855302946105L;
  private Class protoClass;

  public LzoProtobufScheme(Class protoClass) {
    this.protoClass = protoClass;
  }

  @Override
  public void sinkConfInit(HadoopFlowProcess hfp, Tap tap, JobConf conf) {
    conf.setOutputFormat(
      DeprecatedLzoProtobufBlockOutputFormat.getOutputFormatClass(protoClass, conf)
    );
  }

  @Override
  public void sourceConfInit(HadoopFlowProcess hfp, Tap tap, JobConf conf) {
    MultiInputFormat.setClassConf(protoClass, conf);
    DeprecatedInputFormatWrapper.setInputFormat(MultiInputFormat.class, conf);
  }
}
