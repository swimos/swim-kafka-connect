package swim.kafka.connector.sink;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;

public class SwimKafkaSinkTaskSpec {

  private SwimKafkaSinkTask swimKafkaSinkTask;

  @BeforeTest
  public void initTestData() {
    swimKafkaSinkTask = new SwimKafkaSinkTask();
  }

  @Test
  public void getVersion() {
    assertEquals(swimKafkaSinkTask.version(), "3.11.0");
  }
}
