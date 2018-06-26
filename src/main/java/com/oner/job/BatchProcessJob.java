package com.oner.job;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.function.DistributedComparator;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.server.JetBootstrap;
import com.hazelcast.query.Predicates;
import com.oner.model.BiTemporalDoc;
import com.oner.model.RunMode;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.oner.model.BiTemporalDoc.*;
import static java.lang.Runtime.getRuntime;

public class BatchProcessJob {

    public static final String MAP_NAME = "INSTRUMENTS";

    public static void main(String[] args) throws Exception {
        System.out.println("Arguments: " + Arrays.toString(args));
        System.out.println("  " + BatchProcessJob.class.getSimpleName() + " <runMode> <query>");
        System.out.println();
        System.out.println("<runMode> - \"LOCAL\" or \"CLUSTER\", default is \"LOCAL\"");

        RunMode runMode = RunMode.LOCAL;
        if (args.length > 0) runMode = RunMode.valueOf(args[0]);
        String query = args[1];

        JetInstance jet;
        if (RunMode.CLUSTER == runMode) {
            jet = JetBootstrap.getInstance();
        } else {
            System.setProperty("hazelcast.phone.home.enabled", "false");
            System.setProperty("hazelcast.logging.type", "log4j");
            JetConfig cfg = new JetConfig();
            cfg.setInstanceConfig(new InstanceConfig().setCooperativeThreadCount(
                    Math.max(1, getRuntime().availableProcessors() / 2)));
            Jet.newJetInstance(cfg);
            jet = Jet.newJetInstance(cfg);
        }

        try {
            System.out.println("Submitting Job");

            Job job = jet.newJob(buildPipeline(null, null));
            System.out.println("Submitted");

            System.out.println("Press any key to cancel the job");

            System.in.read();

            System.out.println("Cancelling job...");
            job.cancel();

            Thread.sleep(5000);

        } finally {
            Jet.shutdownAll();
        }
    }

    private static Pipeline buildPipeline(LocalDateTime asAtDate, String... sources) {
        Pipeline p = Pipeline.create();


        BatchStage<BiTemporalDoc> data = p.drawFrom(Sources.<BiTemporalDoc, String, BiTemporalDoc>map(MAP_NAME, Predicates.in("source", sources), e -> e.getValue()));

        BatchStage<Map.Entry<String, List<BiTemporalDoc>>> aggregate = data
                .filter(e -> e.isValid(asAtDate) && e.isValidTransaction(asAtDate))
                .filter(e -> e.getCurrency().equals("USD") && e.getMaturityDate().isEqual(LocalDate.now()))
                .groupingKey(BiTemporalDoc::getLogicalId)
                .aggregate(AggregateOperations.maxBy(DistributedComparator.comparing(e -> e.getTransactionTime())))
                .map(Map.Entry::getValue)
                .groupingKey(e -> e.getLogicalId().getDataId())
                .aggregate(AggregateOperations.toList());


        aggregate.drainTo(Sinks.logger());

        return p;
    }
}
