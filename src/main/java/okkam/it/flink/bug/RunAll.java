package okkam.it.flink.bug;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class RunAll implements ProgramDescription {

  private static final class SoggReducer implements ReduceFunction<Person> {
    private static final long serialVersionUID = 1L;

    @Override
    public Person reduce(Person sogg1, Person sogg2) {
      Person ret = sogg1;
      if (ret.getCf().isEmpty() || ret.getCf().equals("null")) {
        ret.setCf(sogg2.getCf());
      }
      if (ret.getSurname().isEmpty() || ret.getSurname().equals("null")) {
        ret.setSurname(sogg2.getSurname());
      }
      if (ret.getName().isEmpty() || ret.getName().equals("null")) {
        ret.setName(sogg2.getName());
      }
      if (ret.getAlternative().isEmpty() || ret.getAlternative().equals("null")) {
        ret.setAlternative(sogg2.getAlternative());
      }
      if (ret.getId().isEmpty() || ret.getId().equals("null")) {
        ret.setId(sogg2.getId());
      }
      return ret;
    }
  }

  @Override
  public String getDescription() {
    return "Test";
  }

  public static void main(String[] args) throws Exception {
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSet<String> text = env.fromElements("Bill,Gates,1,BillGates1,test", //
        "Bill,Gates, ,BillGates1,test", //
        "Fill,Gates,1,BillGates1,test", //
        "Bill,Gates, ,BillGates1,test2", //
        "Bill,Gates,1,BillGates1,test", //
        "Bill,Gates,1, ,test2", //
        "Bill,Gates,1,BillGates1,test", //
        ",Gates,1,BillGates1,test", //
        "Bill,Gates, ,BillGates1,test", //
        "Bill,Gates,1,BillGates1,test", //
        "Gill,Gates,1,BillGates1,test", //
        "Bill,Gates, ,BillGates1,test3", //
        "Bill,Gates,1,BillGates1,test", //
        ",Gates,1,BillGates1,test", //
        "Will,Gates,1,BillGates1,test", //
        "Bill,Gates,2,null,test", //
        "null,null, ,null,test", //
        "null,null,3,null, ", //
        "null,Marvin,3,null,null", //
        "Bill,Gates,null,null,null", //
        "Bill,Gates, ,BillGates1,rec", //
        " , , , , ", //
        " , ,1, , ");

    DataSet<Person> sogg = text.flatMap(readSogg());

    for (int i = 0; i < 5; i++) {
      sogg = sogg.union(text.flatMap(readSogg()));
    }

    DataSet<Person> soggs = sogg//
        .filter(x -> !x.getName().isEmpty())//
        .map(x -> x);

    DataSet<Person> soggWithId = soggs.//
        filter(s -> !s.getId().trim().isEmpty() && !s.getId().equals("null"))//
        .groupBy("id")//
        .reduce(new SoggReducer());

    DataSet<Person> soggWithoutId = soggs//
        .filter(s -> s.getId().trim().isEmpty() || s.getId().equals("null"));

    DataSet<Person> soggs2 = soggWithoutId.union(soggWithId);

    // soggs2 = soggs2.rebalance(); // With rebalancing all works

    // According to the following filters/group/reduce operations,
    // only one record with "cf"!=null and "cf"!=" " should be produced
    // (Only one record should contain cf=BillGates1).
    // This do not happen.
    DataSet<Person> uniqueSoggWithCf = soggs2//
        .filter(s -> !s.getCf().trim().isEmpty() && !s.getCf().equals("null"))//
        .groupBy("cf")//
        .reduce(new SoggReducer());

    DataSet<Person> uniqueSoggWithoutCf = soggs2//
        .filter(s -> s.getCf().trim().isEmpty() || s.getCf().equals("null"))//
        .groupBy("name", "surname", "alternative")//
        .reduce(new SoggReducer());

    DataSet<Person> fullSogg = uniqueSoggWithCf.union(uniqueSoggWithoutCf);


    DataSet<String> jsonSogg = fullSogg.map(new RichMapFunction<Person, String>() {
      private static final long serialVersionUID = 1L;

      @Override
      public String map(Person sogg) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(sogg);
      }
    });

    jsonSogg.writeAsText("/tmp/soggs/", WriteMode.OVERWRITE);
    // BufferedWriter writer = new BufferedWriter(new FileWriter("plan.txt", true));
    // writer.append(env.getExecutionPlan());
    // writer.close();

    env.execute("JSON generation");
  }

  private static FlatMapFunction<String, Person> readSogg() {
    return new FlatMapFunction<String, Person>() {

      private static final long serialVersionUID = 1L;

      @Override
      public void flatMap(String value, Collector<Person> out) throws Exception {
        String[] values = value.split(",");
        Person sogg = new Person();
        sogg.setName(values[0]);
        sogg.setSurname(values[1]);
        sogg.setId(values[2]);
        sogg.setCf(values[3]);
        sogg.setAlternative(values[4]);
        out.collect(sogg);
      }
    };
  }

}
