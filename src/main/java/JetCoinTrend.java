import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.stream.IStreamList;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;

public class JetCoinTrend {
    public static void main(String[] args) throws Exception {

        RedditSource redditSource = new RedditSource("a", "b", "c", "d");
        ProcessorMetaSupplier psReddit = ProcessorMetaSupplier.dontParallelize(redditSource);

        ProcessorMetaSupplier psTwitter = ProcessorMetaSupplier.dontParallelize(new TwitterSource());

        DAG dag = new DAG();
        dag.newVertex("twitter", psTwitter);
        dag.newVertex("reddit", psReddit);
        dag.newVertex("consume", ProcessorMetaSupplier.dontParallelize(new ProcessorSupplier() {
            @Override
            public Collection<? extends Processor> get(int count) {
                AbstractProcessor processor = new AbstractProcessor() {
                    @Override
                    protected boolean tryProcess(int ordinal, Object item) {
                        if (ordinal == 0) {
                            return tryEmit(item);
                        } else {
                            return tryEmit(item);
                        }

                    }
                };
                return Collections.singletonList(processor);
            }
        }));
        dag.edge(Edge.from(dag.getVertex("twitter")).to(dag.getVertex("consume"), 0));
        dag.edge(Edge.from(dag.getVertex("reddit")).to(dag.getVertex("consume"), 1));
        dag.newVertex("sink", SinkProcessors.writeListP("counts"));
        dag.edge(Edge.between(dag.getVertex("consume"), dag.getVertex("sink")));

        // Start Jet, populate the input list
        JetInstance jet = Jet.newJetInstance();
        try {

            // Perform the computation
            jet.newJob(dag);

            // Check the results
            IStreamList<Object> list = jet.getList("counts");

            Thread.sleep(30000);
            list.stream().forEach(System.out::println);
        } finally {
            Jet.shutdownAll();
        }
    }

}