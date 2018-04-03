package okkam.it.flink.flink150SNAPSHOT;

import scala.Serializable;

public class Pojo implements Serializable {
  String name = "";

  public Pojo(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return "Pojo [name=" + name + "]";
  }


}
