package chap12;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;

/**
 * Created by zhoudunxiong on 2018/11/23.
 */
public class JMaximum extends UDAF {
    public static class JMaximumEvaluator implements UDAFEvaluator {

        private IntWritable result;
        @Override
        public void init() {
            result = null;
        }

        public boolean iterate(IntWritable value) {
            if (value == null) {
                return true;
            }
            if (result == null) {
                result.set(value.get());
            } else {
                result.set(Math.max(result.get(), value.get()));
            }
            return true;
        }

        public IntWritable terminatePartial() {
            return result;
        }

        public boolean merge(IntWritable other) {
            return iterate(other);
        }

        public IntWritable terminate() {
            return result;
        }
    }
}
