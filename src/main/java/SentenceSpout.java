import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


public class SentenceSpout extends BaseRichSpout{

    private SpoutOutputCollector collector;
    private ConcurrentHashMap<UUID,Values> pending ;

    private String[] sentences={
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "don't hava a cow man ",
            "i don't think i like fleas"
    };

    private static AtomicLong count =new AtomicLong();
    private long ID=0;

    private int index=0;


    /*
     声明spout会发射一个数据流，其中的tuple包含一个字段sentence
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("sentence"));

    };
    /*
    spout初始化时调用这个方法
    map包含storm配置信息
    TopologyContext对象提供了topology中组件的信息
    SpoutOutputCollector对象提供了发射tuple的方法
     */
    public void open(Map config, TopologyContext context, SpoutOutputCollector collector){
        this.ID= count.addAndGet(1);
        this.collector=collector;
        this.pending = new ConcurrentHashMap<UUID,Values>();

    }

    /*
     Storm通过调用这个方法向输出的collector发射tuple
    */
    public void nextTuple(){

        Values  values  = new Values(sentences[index]);
        UUID msgId = UUID.randomUUID();
        this.pending.put(msgId, values);
        this.collector.emit(values,msgId);
        index++;
        if(index>=sentences.length){
            index=0;
        }
        Utils.sleep(1000);
    }

    @Override
    public void ack(Object msgId) {
        this.pending.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        this.collector.emit(this.pending.get(msgId),msgId);
    }
}