package storm;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * storm 实现加法
 */
public class LocalStorm {
    public static class  DataSoureSpout extends BaseRichSpout{

        private  SpoutOutputCollector collector;
        /**
         *  初始化 ，只会被调用一次
         * @param map    配置参数
         * @param topologyContext  上下文
         * @param spoutOutputCollector  数据发射器
         */
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.collector=spoutOutputCollector;

        }

        int number=0;
        /**
         *  核心方法 ，产生数据，在生产上从消息队列产生数据
         *  这个方法是个死循环
         */
        public void nextTuple() {
            this.collector.emit(new Values(number++,number*2));
            System.out.println("spout: "+number);
            //防止数据产生太快

            // 防止数据产生太快
            Utils.sleep(1000);
        }

        /**
         * 声明输出字段
         * @param outputFieldsDeclarer
         */
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("num1","num2"));
    }
    }

    /**
     * 数据累计求和，bolt:接收数据并处理
     */
    public  static class SumBolt extends BaseRichBolt{
        
        int sum1=0;
        int sum2=0;
        
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        }

        /**
         * 也是一个死循环，获取SPOUT发送来的数据
         * @param tuple
         */
        public void execute(Tuple tuple) {
            Integer value1 = tuple.getIntegerByField("num1");
            Integer value2=tuple.getIntegerByField("num2");
            sum1+=value1;
            sum2+=value2;
            System.out.println(sum1+"  "+sum2);
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }

    public static void main(String[] args) {
        //根据局SPOUT 和BOLT来构建 topology
        //指定SPOUT 和BOLT 执行顺序
        TopologyBuilder builder=new TopologyBuilder();
        //创建一个班底STORM集群
        builder.setSpout("DataSoureSpout",new DataSoureSpout());
        builder.setBolt("SumBolt",new SumBolt()).shuffleGrouping("DataSoureSpout");

        LocalCluster cluster=new LocalCluster();
        cluster.submitTopology("LocalStorm",new Config(),builder.createTopology());
    }
}