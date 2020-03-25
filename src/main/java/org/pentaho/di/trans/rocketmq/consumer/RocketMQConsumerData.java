package org.pentaho.di.trans.rocketmq.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * Holds data processed by this step
 *
 * @author Michael
 */
public class RocketMQConsumerData extends BaseStepData implements StepDataInterface {

  DefaultMQPushConsumer consumer;
  RowMetaInterface outputRowMeta;
  RowMetaInterface inputRowMeta;
  boolean canceled;
  int processed;
}
