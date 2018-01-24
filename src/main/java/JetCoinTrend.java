import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Pipeline;
import com.hazelcast.jet.Sinks;
import com.hazelcast.jet.Sources;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.stream.IStreamList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;

public class JetCoinTrend {
    public static void main(String[] args) throws Exception {
        Pipeline p = Pipeline.create();
        ProcessorMetaSupplier ps = ProcessorMetaSupplier.of(new ProcessorSupplier() {
            @Override
            public Collection<? extends Processor> get(int count) {
                ArrayList<Processor> processors = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    AbstractProcessor abstractProcessor = new AbstractProcessor() {

                        private Stream<String> stream;

                        @Override
                        protected void init(Context context) throws Exception {
                            super.init(context);
                            stream = Stream.generate(() -> "test1");
                        }


                        @Override
                        public boolean complete() {

                            return emitFromTraverser(traverseStream(stream));
                        }

                    };
                    processors.add(abstractProcessor);
                }
                return processors;

            }
        });
        p.drawFrom(Sources.<String>fromProcessor("sancar", ps))
                .map(JetCoinTrend::nlpCompute)
                .drainTo(Sinks.list("counts"));

        // Start Jet, populate the input list
        JetInstance jet = Jet.newJetInstance();
        try {

            // Perform the computation
            jet.newJob(p);

            // Check the results
            IStreamList<Object> list = jet.getList("counts");

            Thread.sleep(2000);
            list.stream().forEach(System.out::println);
        } finally {
            Jet.shutdownAll();
        }
    }

    static double nlpCompute(String word) {
        return 2.0;
    }
}