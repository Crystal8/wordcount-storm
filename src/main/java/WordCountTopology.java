import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class WordCountTopology {

    private static final String SENTENCE_SPOUT_ID="sentence-spout";
    private static final String SPILL_BOLT_ID ="split-bolt";
    private static final String COUNT_BOLT_ID ="count-bolt";
    private static final String REPORT_BOLT_ID="report-bolt";
    private static final String TOPOLOGY_NAME="wordcount";

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

        SentenceSpout spout = new SentenceSpout();
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();
        WordCountBolt countBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();

        TopologyBuilder builder = new TopologyBuilder();

        //注册一个sentence spout并且赋值给其唯一的ID，2表示需要2个executor来运行该spout
        //将2改为1会发现计数结果将近少了一半
        builder.setSpout(SENTENCE_SPOUT_ID, spout, 2);
        //注册一个splitsentencebolt ，这个bolt订阅sentencespout发射出来的数据流,shuffleGrouping方法告诉
        //storm要将类sentenceSpout发射的tuple随机均匀地分发给SplitSentenceBolt实例，此处的2表示2个executor
        //setNumTasks设置了4个task，因此一个executor运行2个task
        builder.setBolt(SPILL_BOLT_ID,splitBolt,2)
                .setNumTasks(4)
                .shuffleGrouping(SENTENCE_SPOUT_ID);
        //fieldsGrouping()方法来保证所有 word字段值相同的tuple会被路由到同一个wordcountbolt实例中
        builder.setBolt(COUNT_BOLT_ID, countBolt, 4)
                .fieldsGrouping(SPILL_BOLT_ID, new Fields("word"));
        //globalGrouping方法将WordCountBolt发射的所有tuple路由到唯一的ReportBolt任务中
        builder.setBolt(REPORT_BOLT_ID,reportBolt).globalGrouping(COUNT_BOLT_ID);

        //config对象代表了对topology所有组件全局生效的配置参数集合，会分发给各个spout和bolt的open(),prepare()方法
        Config config = new Config();
        config.setNumWorkers(2);

        //使用命令行参数来决定使用 本地/远程集群模式
        if(args.length==0) {
            //LocalCluster类在本地开发环境来模拟一个完整的storm集群
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();
        }else {
            try {
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            }
            catch(AuthorizationException exp) {
                //
            }
        }

    }
}