package net.tcs.db.adapter;

import org.javalite.activejdbc.StaleModelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tcs.drivers.TCSDriver;

public abstract class RetryOnStaleDecorator<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetryOnStaleDecorator.class);

    static final int RETRY_COUNT_ON_STALED_DATA = 3;

    public abstract T executeDB();

    public T execute(String expectedError) {

        for (int i = 0; i < RETRY_COUNT_ON_STALED_DATA; i++) {
            boolean succeeded = true;
            TCSDriver.getDbAdapter().openConnection();

            try {
                TCSDriver.getDbAdapter().beginTX();

                return executeDB();

            } catch (final StaleModelException ex) {
                succeeded = false;
                if (i < RETRY_COUNT_ON_STALED_DATA - 1) {
                    LOGGER.warn("StaleModelException while " + expectedError + ". Retrying", ex);
                } else {
                    LOGGER.warn("StaleModelException while " + expectedError, ex);
                }
            } catch (final RuntimeException re) {
                succeeded = false;
                throw re;
            } finally {
                if (succeeded) {
                    TCSDriver.getDbAdapter().commitTX();
                } else {
                    TCSDriver.getDbAdapter().rollbackTX();
                }
                TCSDriver.getDbAdapter().closeConnection();
            }
        }
        return null;
    }

    public T executeNoTransaction(String expectedError) {

        for (int i = 0; i < RETRY_COUNT_ON_STALED_DATA; i++) {
            TCSDriver.getDbAdapter().openConnection();

            try {
                return executeDB();
            } catch (final StaleModelException ex) {
                if (i < RETRY_COUNT_ON_STALED_DATA - 1) {
                    LOGGER.warn("StaleModelException while " + expectedError + ". Retrying", ex);
                } else {
                    LOGGER.warn("StaleModelException while " + expectedError, ex);
                }
            } finally {
                TCSDriver.getDbAdapter().closeConnection();
            }
        }
        return null;
    }
}
