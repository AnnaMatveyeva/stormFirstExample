package stormexample;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import stormexample.bolt.EmailCounter;
import stormexample.bolt.EmailExtractor;
import stormexample.spout.CommitFeedListener;

public class LocalTopologyRunner {

    private static final int ONE_MINUTES = 6000;

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("commit-feed-listener", new CommitFeedListener());
        builder.setBolt("email-extractor", new EmailExtractor())
            .shuffleGrouping("commit-feed-listener");
        builder.setBolt("email-counter", new EmailCounter())
            .fieldsGrouping("email-extractor", new Fields("email"));

        Config config = new Config();
        config.setDebug(true);

        StormTopology stormTopology = builder.createTopology();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("github-commit-count-topology", config, stormTopology);
        Utils.sleep(ONE_MINUTES);
        cluster.killTopology("github-commit-count-topology");
        cluster.shutdown();

    }

}
