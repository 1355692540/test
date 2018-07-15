package storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.shade.org.apache.commons.io.FileUtils;
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

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * storm 实现词频统计
 */
public class LocalWordCount {
    public static class DataSourceSpout extends BaseRichSpout{

        private  SpoutOutputCollector collector;
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.collector=spoutOutputCollector;
        }

        /**
         * 业务
         * 1：读取指定文件夹下的数据
         * 2：把数据发送出去
         */
        public void nextTuple() {
            //获取所有文件
            Collection<File> files = FileUtils.listFiles(new File("D:\\"), new String[]{"txt"}, true);

            for (File file:files){
                try {
                    //获取文件内容
                    List<String> strings = FileUtils.readLines(file, "UTF-8");
                    //获取每一行的内容
                    for (String line:strings){
                        this.collector.emit(new Values(line));
                    }
                    //// TODO: 2018/6/11  数据处理完后，改名，否则永远死循环
                    FileUtils.moveFile(file,new File(file.getAbsolutePath()+System.currentTimeMillis()));
                } catch (IOException e) {

                }
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("line"));
        }
    }

    public static class SplitBolt extends BaseRichBolt{

        private OutputCollector otputCollector;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.otputCollector=outputCollector;
        }

        /**
         * 按照,进行分割
         * @param tuple
         */
        public void execute(Tuple tuple) {
            String line=tuple.getStringByField("line");
            String[] split = line.split(",");
            for (String word:split){
                this.otputCollector.emit(new Values(word));
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields(("word")));
        }
    }

    public static class CountBolt extends BaseRichBolt{

        Map<String,Integer> map=new HashMap();
        /**
         * 获取每个单词，对单词进行汇总，输出
         *
         * @param map
         * @param topologyContext
         * @param outputCollector
         */
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        }

        public void execute(Tuple tuple) {
             String word = tuple.getStringByField("word");
            Integer count=map.get(word);
            if (count==null){
                count=0;
            }
            count++;

            map.put(word,count);
            //输出
            Set<Map.Entry<String, Integer>> entries = map.entrySet();
            for (Map.Entry entry:entries){
                System.out.println(entry);
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }

    public static void main(String[] args) {
        //通过topologyBuilder 创建
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("DataSourceSpout",new DataSourceSpout());
        builder.setBolt("SplitBolt",new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt",new CountBolt()).shuffleGrouping("SplitBolt");

        //创建本地集群
        LocalCluster cluster=new LocalCluster();
        cluster.submitTopology("LocalWordCount",new Config(),builder.createTopology());
    }
}