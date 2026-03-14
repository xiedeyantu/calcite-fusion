package github.xiedeyantu.pipeline;

/**
 * Push-model consumer: an operator that receives columnar batches from its upstream.
 *
 * <p>In the push pipeline, the data source drives execution and calls
 * {@link #consume} for each batch it produces. When the source is exhausted it
 * calls {@link #done} so pipeline-breakers (Sort, HashAgg) can emit their
 * buffered output.
 */
public interface Consumer {

    /** Receive and process one columnar batch. */
    void consume(ColumnBatch batch);

    /**
     * Signal that the upstream source is exhausted.
     * Pipeline-breakers finalize and push results to their downstream here.
     */
    void done();
}
