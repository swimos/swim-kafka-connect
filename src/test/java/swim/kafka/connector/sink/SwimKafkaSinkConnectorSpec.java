package swim.kafka.connector.sink;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;

public class SwimKafkaSinkConnectorSpec {

  private SwimKafkaSinkConnector swimKafkaSinkConnector;

  @BeforeTest
  public void initTestData() {
    swimKafkaSinkConnector = new SwimKafkaSinkConnector();
  }

  @Test
  public void config() {
    assertEquals(swimKafkaSinkConnector.config(), SwimSinkConfig.SWIM_SINK_CONFIG_DEF);
  }

  @Test
  public void taskClass() {
    assertEquals(swimKafkaSinkConnector.taskClass(), SwimKafkaSinkTask.class);
  }

  @Test
  public void version() {
    assertEquals(swimKafkaSinkConnector.version(), "3.11.0");
  }
}
