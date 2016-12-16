import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SplitSentenceBolt extends BaseRichBolt{

    private OutputCollector collector;

    /*
     bolt初始化时调用
     */
    public void prepare(Map config ,TopologyContext context,OutputCollector collector){
        this.collector = collector;
    }

    /*
     声明每个 tuple包含一个字段 word
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("word"));
    }

    /*
     每当从订阅的数据流中接收一个tuple，都会调用这个方法
     */
    public void execute(Tuple tuple){
        String sentence = tuple.getStringByField("sentence");
        String[] words = sentence.split(" ");
        for(String word : words){
            //如果OutputCollector.emit(oldTuple, newTuple)这样调用来发射tuple(在storm中称之为anchoring),
            // 那么后面的bolt的ack/fail会影响spout的ack/fail, 如果collector.emit(newTuple)这样来发射tuple(在storm称之为unanchoring),
            // 则相当于断开了后面bolt的ack/fail对spout的影响.spout将立即根据当前bolt前面的ack/fail的情况来决定调用spout的ack/fail.
            // 所以某个bolt后面的bolt的成功失败对你来说不关心, 你可以直接通过这种方式来忽略
            this.collector.emit(tuple,new Values(word));
            //this.collector.emit(new Values(word));
        }
        this.collector.ack(tuple);
    }

}