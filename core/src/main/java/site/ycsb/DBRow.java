package site.ycsb;

import java.util.Map;

/**
 * @author liujinyong
 * @create 2023/5/11 1:40 PM
 */
public class DBRow {
  private final String key;
  private final Map<String, ByteIterator> fieldMap;

  public DBRow(String key, Map<String, ByteIterator> fieldMap) {
    this.key = key;
    this.fieldMap = fieldMap;
  }

  public String getKey() {
    return key;
  }

  public Map<String, ByteIterator> getFieldMap() {
    return fieldMap;
  }
}
