package okkam.it.flink.bug;

public class Person {

  String id = "";
  String cf = "";
  String name = "";
  String surname = "";
  String alternative = "";

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getCf() {
    return cf;
  }

  public void setCf(String cf) {
    this.cf = cf;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getSurname() {
    return surname;
  }

  public void setSurname(String surname) {
    this.surname = surname;
  }

  public String getAlternative() {
    return alternative;
  }

  public void setAlternative(String alternative) {
    this.alternative = alternative;
  }

  @Override
  public String toString() {
    return "Soggetto [id=" + id + ", cf=" + cf + ", name=" + name + ", surname=" + surname
        + ", alternative=" + alternative + "]";
  }

}
