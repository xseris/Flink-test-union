package okkam.it.flink.flink150SNAPSHOT;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class RandomCsvGenerator {

  private static final int ROWS = 50000000;
  private static final int COLS = 10;
  private static final int WORD_LENGTH = 15;

  public static void main(String[] args) throws IOException {

    BufferedWriter writer = new BufferedWriter(new FileWriter("test.csv"));
    for (int i = 0; i < ROWS; i++) {
      StringBuilder sb = new StringBuilder();
      for (int j = 0; j < COLS; j++) {
        if (j > 0) {
          sb.append(",");
        }
        sb.append(buildStringOfLength(WORD_LENGTH));
      }
      writer.write(sb.append("\n").toString());
    }

    writer.close();
  }

  public static String buildStringOfLength(int length) {
    StringBuilder sb = new StringBuilder();
    char[] chars = "abcde".toCharArray();
    for (int i = 0; i < length; i++) {
      int rand = (int) (Math.random() * chars.length);
      sb.append(String.valueOf(chars[rand]));
    }
    return sb.toString();
  }
}
