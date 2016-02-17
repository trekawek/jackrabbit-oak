package org.apache.jackrabbit.oak.resilience.junit;

import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingRunListener extends RunListener {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingRunListener.class);

    @Override
    public void testRunStarted(Description description) throws Exception {
        LOG.info("Started run {}", description.getDisplayName());
    }

    @Override
    public void testRunFinished(Result result) throws Exception {
        LOG.info("Finished run. Total time: {}, total tests: {}, successful: {}", result.getRunTime(),
                result.getRunCount(), result.wasSuccessful());
    }

    @Override
    public void testStarted(Description description) throws Exception {
        LOG.info("Started test {}", description.getDisplayName());
    }

    @Override
    public void testFinished(Description description) throws Exception {
        LOG.info("Finished test {}", description.getDisplayName());
    }

    @Override
    public void testFailure(Failure failure) throws Exception {
        LOG.info("Test {} failed: {}", failure.getDescription().getDisplayName(), failure.getTrace());
    }

    @Override
    public void testAssumptionFailure(Failure failure) {
        LOG.info("Assumption failed for {}", failure.getDescription().getDisplayName());
    }

    @Override
    public void testIgnored(Description description) throws Exception {
        LOG.info("Test ignored {}", description.getDisplayName());
    }
}
