package github.xiedeyantu.pipeline;

/**
 * A pipeline source that drives execution by pushing batches to its downstream.
 *
 * <p>Only scan operators implement this interface — every other operator is a
 * {@link Consumer} that sits in the push chain between a source and the final
 * {@code ResultCollector}.
 */
public interface Source {

    /**
     * Read all data and push it — batch by batch — to the downstream consumer
     * configured at construction time. Calls {@link Consumer#done()} when
     * finished so that pipeline-breakers can emit buffered results.
     */
    void execute();
}
