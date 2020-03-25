package org.pentaho.di.trans.rocketmq.consumer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * rocket Consumer step definitions and serializer to/from XML and to/from Kettle repository.
 *
 * @author cunxiaopan
 */
@Step(
    id = "RocketMQConsumer",
    image = "org/pentaho/di/trans/rocketmq/consumer/resources/RocketMQConsumer.svg",
    i18nPackageName = "org.pentaho.di.trans.rocketmq.consumer",
    name = "RocketMQConsumerDialog.Shell.Title",
    description = "RocketMQConsumerDialog.Shell.Tooltip",
    documentationUrl = "RocketMQConsumerDialog.Shell.DocumentationURL",
    casesUrl = "RocketMQConsumerDialog.Shell.CasesURL",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Message")
public class RocketMQConsumerMeta extends BaseStepMeta implements StepMetaInterface {

  @SuppressWarnings("WeakerAccess")
  protected static final String[] ROCKET_MQ_PROPERTIES_NAMES = new String[]{"server.connect", "subExpression"};

  @SuppressWarnings("WeakerAccess")
  protected static final Map<String, String> ROCKETMQ_PROPERTIES_DEFAULTS = new HashMap<String, String>();

  private static final String ATTR_GROUP_NAME = "GROUPNAME";
  private static final String ATTR_TOPIC = "TOPIC";
  private static final String ATTR_FIELD = "FIELD";
  private static final String ATTR_KEY_FIELD = "KEY_FIELD";
  private static final String ATTR_LIMIT = "LIMIT";
  private static final String ATTR_STOP_ON_EMPTY_TOPIC = "STOP_ON_EMPTY_TOPIC";
  private static final String ATTR_KAFKA = "ROCKETMQ";

  static {
    ROCKETMQ_PROPERTIES_DEFAULTS.put("server.connect", "localhost:9876");
    ROCKETMQ_PROPERTIES_DEFAULTS.put("subExpression", "*");
  }

  private Properties properties = new Properties();
  private String topic;
  private String field;
  private String keyField;
  private String limit;
  private String groupName;
  private boolean stopOnEmptyTopic;

  public static String[] getPropertiesNames() {
    return ROCKET_MQ_PROPERTIES_NAMES;
  }

  public static Map<String, String> getPropertiesDefaults() {
    return ROCKETMQ_PROPERTIES_DEFAULTS;
  }

  public RocketMQConsumerMeta() {
    super();
  }

  public Properties getProperties() {
    return properties;
  }

  @SuppressWarnings("unused")
  public Map getPropertiesMap() {
    return getProperties();
  }

  public void setProperties(Properties rocketroperties) {
    this.properties = rocketroperties;
  }

  @SuppressWarnings("unused")
  public void setPropertiesMap(Map<String, String> propertiesMap) {
    Properties props = new Properties();
    props.putAll(propertiesMap);
    setProperties(props);
  }

  /**
   * @return rocket mq topic name
   */
  public String getTopic() {
    return topic;
  }

  /**
   * @param topic rocket mq topic name
   */
  public void setTopic(String topic) {
    this.topic = topic;
  }

  /**
   * @return Target field name in rocket mq stream
   */
  public String getField() {
    return field;
  }

  /**
   * @param field Target field name in Kettle stream
   */
  public void setField(String field) {
    this.field = field;
  }

  /**
   * @return Target key field name in Kettle stream
   */
  public String getKeyField() {
    return keyField;
  }

  /**
   * @param keyField Target key field name in Kettle stream
   */
  public void setKeyField(String keyField) {
    this.keyField = keyField;
  }

  /**
   * @return groupName  get group name from rocketMQ
   */
  public String getGroupName() {
    return groupName;
  }

  /**
   * @param groupName rocketMQ group name
   */
  public void setGroupName(String groupName) {
    this.groupName = groupName;
  }

  /**
   * @return Limit number of entries to read from rocketMQ queue
   */
  public String getLimit() {
    return limit;
  }

  /**
   * @param limit Limit number of entries to read from rocketMQ queue
   */
  public void setLimit(String limit) {
    this.limit = limit;
  }

  /**
   * @return 'true' if the consumer should stop when no more messages are available
   */
  public boolean isStopOnEmptyTopic() {
    return stopOnEmptyTopic;
  }

  /**
   * @param stopOnEmptyTopic If 'true', stop the consumer when no more messages are available on the topic
   */
  public void setStopOnEmptyTopic(boolean stopOnEmptyTopic) {
    this.stopOnEmptyTopic = stopOnEmptyTopic;
  }

  @Override
  public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
      String[] input, String[] output, RowMetaInterface info, VariableSpace space, Repository repository,
      IMetaStore metaStore) {
    if (groupName == null) {
      remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
          Messages.getString("RocketMQConsumerMeta.Check.InvalidGroupNameField"), stepMeta));
    }
    if (topic == null) {
      remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
          Messages.getString("RocketMQConsumerMeta.Check.InvalidTopic"), stepMeta));
    }
    if (field == null) {
      remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
          Messages.getString("RocketMQConsumerMeta.Check.InvalidField"), stepMeta));
    }
    if (keyField == null) {
      remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
          Messages.getString("RocketMQConsumerMeta.Check.InvalidKeyField"), stepMeta));
    }
  }

  @Override
  public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
      Trans trans) {
    return new RocketMQConsumer(stepMeta, stepDataInterface, cnr, transMeta, trans);
  }

  @Override
  public StepDataInterface getStepData() {
    return new RocketMQConsumerData();
  }

  @Override
  public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore)
      throws KettleXMLException {

    try {
      groupName = XMLHandler.getTagValue(stepnode, ATTR_GROUP_NAME);
      topic = XMLHandler.getTagValue(stepnode, ATTR_TOPIC);
      field = XMLHandler.getTagValue(stepnode, ATTR_FIELD);
      keyField = XMLHandler.getTagValue(stepnode, ATTR_KEY_FIELD);
      limit = XMLHandler.getTagValue(stepnode, ATTR_LIMIT);
      // This tag only exists if the value is "true", so we can directly
      // populate the field
      stopOnEmptyTopic = XMLHandler.getTagValue(stepnode, ATTR_STOP_ON_EMPTY_TOPIC) != null;
      Node kafkaNode = XMLHandler.getSubNode(stepnode, ATTR_KAFKA);
      String[] kafkaElements = XMLHandler.getNodeElements(kafkaNode);
      if (kafkaElements != null) {
        for (String propName : kafkaElements) {
          String value = XMLHandler.getTagValue(kafkaNode, propName);
          if (value != null) {
            properties.put(propName, value);
          }
        }
      }
    } catch (Exception e) {
      throw new KettleXMLException(Messages.getString("KafkaConsumerMeta.Exception.loadXml"), e);
    }
  }

  @Override
  public String getXML() throws KettleException {
    StringBuilder retval = new StringBuilder();
    if (topic != null) {
      retval.append("    ").append(XMLHandler.addTagValue(ATTR_TOPIC, topic));
    }
    if (field != null) {
      retval.append("    ").append(XMLHandler.addTagValue(ATTR_FIELD, field));
    }
    if (keyField != null) {
      retval.append("    ").append(XMLHandler.addTagValue(ATTR_KEY_FIELD, keyField));
    }
    if (groupName != null) {
      retval.append("    ").append(XMLHandler.addTagValue(ATTR_GROUP_NAME, groupName));
    }
    if (limit != null) {
      retval.append("    ").append(XMLHandler.addTagValue(ATTR_LIMIT, limit));
    }
    if (stopOnEmptyTopic) {
      retval.append("    ").append(XMLHandler.addTagValue(ATTR_STOP_ON_EMPTY_TOPIC, "true"));
    }
    retval.append("    ").append(XMLHandler.openTag(ATTR_KAFKA)).append(Const.CR);
    for (String name : properties.stringPropertyNames()) {
      String value = properties.getProperty(name);
      if (value != null) {
        retval.append("      ").append(XMLHandler.addTagValue(name, value));
      }
    }
    retval.append("    ").append(XMLHandler.closeTag(ATTR_KAFKA)).append(Const.CR);
    return retval.toString();
  }

  @Override
  public void readRep(Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases)
      throws KettleException {
    try {
      topic = rep.getStepAttributeString(stepId, ATTR_TOPIC);
      field = rep.getStepAttributeString(stepId, ATTR_FIELD);
      keyField = rep.getStepAttributeString(stepId, ATTR_KEY_FIELD);
      groupName = rep.getStepAttributeString(stepId, ATTR_GROUP_NAME);
      limit = rep.getStepAttributeString(stepId, ATTR_LIMIT);
      stopOnEmptyTopic = rep.getStepAttributeBoolean(stepId, ATTR_STOP_ON_EMPTY_TOPIC);
      String kafkaPropsXML = rep.getStepAttributeString(stepId, ATTR_KAFKA);
      if (kafkaPropsXML != null) {
        properties.loadFromXML(new ByteArrayInputStream(kafkaPropsXML.getBytes()));
      }
      // Support old versions:
      for (String name : ROCKET_MQ_PROPERTIES_NAMES) {
        String value = rep.getStepAttributeString(stepId, name);
        if (value != null) {
          properties.put(name, value);
        }
      }
    } catch (Exception e) {
      throw new KettleException("KafkaConsumerMeta.Exception.loadRep", e);
    }
  }

  @Override
  public void saveRep(Repository rep, IMetaStore metaStore, ObjectId transformationId, ObjectId stepId) throws KettleException {
    try {
      if (topic != null) {
        rep.saveStepAttribute(transformationId, stepId, ATTR_TOPIC, topic);
      }
      if (field != null) {
        rep.saveStepAttribute(transformationId, stepId, ATTR_FIELD, field);
      }
      if (keyField != null) {
        rep.saveStepAttribute(transformationId, stepId, ATTR_KEY_FIELD, keyField);
      }
      if (groupName != null) {
        rep.saveStepAttribute(transformationId, stepId, ATTR_GROUP_NAME, groupName);
      }
      if (limit != null) {
        rep.saveStepAttribute(transformationId, stepId, ATTR_LIMIT, limit);
      }
      rep.saveStepAttribute(transformationId, stepId, ATTR_STOP_ON_EMPTY_TOPIC, stopOnEmptyTopic);

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      properties.storeToXML(buf, null);
      rep.saveStepAttribute(transformationId, stepId, ATTR_KAFKA, buf.toString());
    } catch (Exception e) {
      throw new KettleException("KafkaConsumerMeta.Exception.saveRep", e);
    }
  }

  /**
   * Set default values to the transformation
   */
  @Override
  public void setDefault() {
    setTopic("");
  }

  @Override
  public void getFields(RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
      VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {

    try {
      ValueMetaInterface fieldValueMeta = ValueMetaFactory.createValueMeta(getField(), ValueMetaInterface.TYPE_BINARY);
      fieldValueMeta.setOrigin(origin);
      rowMeta.addValueMeta(fieldValueMeta);

      ValueMetaInterface keyFieldValueMeta = ValueMetaFactory.createValueMeta(getKeyField(), ValueMetaInterface.TYPE_BINARY);
      keyFieldValueMeta.setOrigin(origin);
      rowMeta.addValueMeta(keyFieldValueMeta);

    } catch (KettlePluginException e) {
      throw new KettleStepException("KafkaConsumerMeta.Exception.getFields", e);
    }

  }

  public static boolean isEmpty(String str) {
    return str == null || str.length() == 0;
  }

}
