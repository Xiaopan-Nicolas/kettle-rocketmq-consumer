package org.pentaho.di.trans.rocketmq.consumer;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * Rocket mq Consumer step processor
 *
 * @author cunxiaopan
 */
public class RocketMQConsumer extends BaseStep implements StepInterface {

  private static Object o = new Object();
  /**
   * 当前消息数量
   */
  private int msgSize = 0;
  /**
   * 最大消息数量
   */
  private int msgMaxSize = 0;

  public RocketMQConsumer(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans) {
    super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
  }

  @Override
  public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
    super.init(smi, sdi);

    RocketMQConsumerMeta meta = (RocketMQConsumerMeta) smi;
    RocketMQConsumerData data = (RocketMQConsumerData) sdi;

    Properties properties = meta.getProperties();
    Properties substProperties = new Properties();
    for (Entry<Object, Object> e : properties.entrySet()) {
      substProperties.put(e.getKey(), environmentSubstitute(e.getValue().toString()));
    }

    logBasic(Messages.getString("RocketMQConsumer.CreateConsumer.Message", substProperties.getProperty("server.connect")));

    // Instantiate with specified consumer group name.
    data.consumer = new DefaultMQPushConsumer(meta.getGroupName());
    // Specify name server addresses.
    data.consumer.setNamesrvAddr(substProperties.getProperty("server.connect"));
    // Subscribe one more more topics to consume.
    try {
      data.consumer.subscribe(meta.getTopic(), substProperties.getProperty("subExpression"));
    } catch (MQClientException e) {
      logError("Rocket MQ 订阅topic失败！", e);
      e.printStackTrace();
    }
    // limit message number
    if(StringUtils.isNoneBlank(meta.getLimit())){
      msgMaxSize = Integer.parseInt(meta.getLimit());
    }
    return true;
  }

  public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
    RocketMQConsumerData data = (RocketMQConsumerData) sdi;
    if (data.consumer != null) {
      data.consumer.shutdown();

    }
    super.dispose(smi, sdi);
  }

  /**
   * 解析msg
   */
  public boolean parseMsgs(List<MessageExt> msgs, RocketMQConsumerData data, RocketMQConsumerMeta meta) {
    try {
      for (MessageExt me : msgs) {
        if (++msgSize > msgMaxSize) {
          return false;
        }

        String msg = new String(me.getBody(), RemotingHelper.DEFAULT_CHARSET);
        logBasic(">>>>>接收消息:" + msg);

        Object[] newRow= RowDataUtil.createResizedCopy( new Object[0], data.outputRowMeta.size() );
        newRow[0] = msg;
        if (isRowLevel()) {
          logRowlevel(Messages.getString("RocketMQConsumer.Log.OutputRow",
              Long.toString(getLinesWritten()), data.outputRowMeta.getString(newRow)));
        }
        List<ValueMetaInterface> list = data.outputRowMeta.getValueMetaList();
        for (ValueMetaInterface item:list) {
          item.setType(2);
        }
        putRow(data.outputRowMeta, newRow);
      }
    } catch (UnsupportedEncodingException e) {
      logError("UnsupportedEncodingException", e);
    } catch (Exception e) {
      logError("Exception", e);
    }
    return true;
  }

  public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
    Object[] r = getRow();
    if (r == null) {
      /*
       * If we have no input rows, make sure we at least run once to
       * produce output rows. This allows us to consume without requiring
       * an input step.
       */
      if (!first) {
        setOutputDone();
        return false;
      }
      r = new Object[0];
    } else {
      incrementLinesRead();
    }
    final Object[] inputRow = r;
    final RocketMQConsumerMeta meta = (RocketMQConsumerMeta) smi;
    final RocketMQConsumerData data = (RocketMQConsumerData) sdi;
    try {
      if (first) {
        first = false;
        if (data.inputRowMeta == null) {
          data.outputRowMeta = new RowMeta();
          data.inputRowMeta = new RowMeta();
        } else {
          data.outputRowMeta = getInputRowMeta().clone();
        }
        meta.getFields(data.outputRowMeta, getStepname(), null, null, this, null, null);
      }
      logDebug("Starting message consumption ...");
      data.consumer.registerMessageListener(new MessageListenerConcurrently() {
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
          synchronized (o) {
            if (parseMsgs(msgs, data,meta)) {
              if (msgSize >= msgMaxSize) {
                o.notifyAll();
              }
              return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            } else {
              o.notifyAll();
              return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
          }
        }
      });
      try {
        data.consumer.start();
      } catch (MQClientException e) {
        logError(">>>>>启动 Rocket MQ Consumer 失败！", e);
        throw new KettleException(e);
      }
      logBasic(">>>>>启动 Rocket MQ Consumer！");
      synchronized (o) {
        try {
          logBasic(">>>>>等待接收mq消息!");
          o.wait();
          logBasic(">>>>>消息接收完毕，5秒后将关闭consumer！");
          TimeUnit.SECONDS.sleep(5);
          data.consumer.shutdown();
          logBasic(">>>>>消息接收完毕，停止Consumer!");
          setOutputDone();
          return false;
        } catch (InterruptedException e) {
          logError("InterruptedException", e);
        }
      }
    } catch (KettleException e) {
      if (!getStepMeta().isDoingErrorHandling()) {
        logError(Messages.getString("RocketMQConsumer.ErrorInStepRunning", e.getMessage()));
        setErrors(1);
        stopAll();
        setOutputDone();
        return false;
      }
      putError(getInputRowMeta(), r, 1, e.toString(), null, getStepname());
    }
    return true;
  }

  public void stopRunning(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

    RocketMQConsumerData data = (RocketMQConsumerData) sdi;
    data.consumer.shutdown();
    data.canceled = true;

    super.stopRunning(smi, sdi);
  }
}
