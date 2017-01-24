package net.tcs.db.adapter;

import static org.mockito.Mockito.times;

import org.javalite.activejdbc.StaleModelException;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import net.tcs.db.ActiveJDBCAdapter;
import net.tcs.db.adapter.RetryOnStaleDecorator;
import net.tcs.drivers.TCSDriver;

public class RetryOnStaleDecoratorTest {
    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testBasic() {
        final ActiveJDBCAdapter mockAdapter = Mockito.mock(ActiveJDBCAdapter.class);
        TCSDriver.setDbAdapter(mockAdapter);

        final RetryOnStaleDecorator<String> foo = new RetryOnStaleDecorator<String>() {

            @Override
            public String executeDB() {
                // TODO Auto-generated method stub
                return null;
            }
        };

        foo.execute("test error");

        Mockito.verify(mockAdapter, times(1)).beginTX();
        Mockito.verify(mockAdapter, times(1)).commitTX();
        Mockito.verify(mockAdapter, times(1)).openConnection();
        Mockito.verify(mockAdapter, times(1)).closeConnection();
    }

    @Test
    public void testBasicThrowsStaleModelExceptionAndThenSucceeds() {
        final ActiveJDBCAdapter mockAdapter = Mockito.mock(ActiveJDBCAdapter.class);
        TCSDriver.setDbAdapter(mockAdapter);

        final RetryOnStaleDecorator<String> foo = new RetryOnStaleDecorator<String>() {

            int count = 0;
            @Override
            public String executeDB() {
                if (count++ < 2) {
                    throw new StaleModelException("test error");
                }
                return null;
            }
        };

        foo.execute("test error");

        Mockito.verify(mockAdapter, times(3)).beginTX();
        Mockito.verify(mockAdapter, times(2)).rollbackTX();
        Mockito.verify(mockAdapter, times(1)).commitTX();
        Mockito.verify(mockAdapter, times(3)).openConnection();
        Mockito.verify(mockAdapter, times(3)).closeConnection();
    }

    @Test
    public void testBasicAlwaysThrowsStaleModelException() {
        final ActiveJDBCAdapter mockAdapter = Mockito.mock(ActiveJDBCAdapter.class);
        TCSDriver.setDbAdapter(mockAdapter);

        final RetryOnStaleDecorator<String> foo = new RetryOnStaleDecorator<String>() {

            @Override
            public String executeDB() {
                throw new StaleModelException("test error");
            }
        };

        foo.execute("test error");

        Mockito.verify(mockAdapter, times(RetryOnStaleDecorator.RETRY_COUNT_ON_STALED_DATA)).beginTX();
        Mockito.verify(mockAdapter, times(RetryOnStaleDecorator.RETRY_COUNT_ON_STALED_DATA)).rollbackTX();
        Mockito.verify(mockAdapter, times(RetryOnStaleDecorator.RETRY_COUNT_ON_STALED_DATA)).openConnection();
        Mockito.verify(mockAdapter, times(RetryOnStaleDecorator.RETRY_COUNT_ON_STALED_DATA)).closeConnection();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testBasicThrowsRuntimeException() {
        final ActiveJDBCAdapter mockAdapter = Mockito.mock(ActiveJDBCAdapter.class);
        TCSDriver.setDbAdapter(mockAdapter);

        final RetryOnStaleDecorator<String> foo = new RetryOnStaleDecorator<String>() {

            @Override
            public String executeDB() {
                throw new IllegalArgumentException("test error");
            }
        };

        foo.execute("test error");

        Mockito.verify(mockAdapter, times(1)).beginTX();
        Mockito.verify(mockAdapter, times(1)).rollbackTX();
        Mockito.verify(mockAdapter, times(1)).openConnection();
        Mockito.verify(mockAdapter, times(1)).closeConnection();
    }

    @Test
    public void testBasicNoTransaction() {
        final ActiveJDBCAdapter mockAdapter = Mockito.mock(ActiveJDBCAdapter.class);
        TCSDriver.setDbAdapter(mockAdapter);

        final RetryOnStaleDecorator<String> foo = new RetryOnStaleDecorator<String>() {

            @Override
            public String executeDB() {
                // TODO Auto-generated method stub
                return null;
            }
        };

        foo.executeNoTransaction("test error");

        Mockito.verify(mockAdapter, times(0)).beginTX();
        Mockito.verify(mockAdapter, times(0)).commitTX();
        Mockito.verify(mockAdapter, times(1)).openConnection();
        Mockito.verify(mockAdapter, times(1)).closeConnection();
    }

    @Test
    public void testBasicNoTransactionThrowsStaleModelExceptionAndThenSucceeds() {
        final ActiveJDBCAdapter mockAdapter = Mockito.mock(ActiveJDBCAdapter.class);
        TCSDriver.setDbAdapter(mockAdapter);

        final RetryOnStaleDecorator<String> foo = new RetryOnStaleDecorator<String>() {

            int count = 0;

            @Override
            public String executeDB() {
                if (count++ < 2) {
                    throw new StaleModelException("test error");
                }
                return null;
            }
        };

        foo.executeNoTransaction("test error");

        Mockito.verify(mockAdapter, times(0)).beginTX();
        Mockito.verify(mockAdapter, times(0)).rollbackTX();
        Mockito.verify(mockAdapter, times(0)).commitTX();
        Mockito.verify(mockAdapter, times(3)).openConnection();
        Mockito.verify(mockAdapter, times(3)).closeConnection();
    }

    @Test
    public void testBasicNoTransactionAlwaysThrowsStaleModelException() {
        final ActiveJDBCAdapter mockAdapter = Mockito.mock(ActiveJDBCAdapter.class);
        TCSDriver.setDbAdapter(mockAdapter);

        final RetryOnStaleDecorator<String> foo = new RetryOnStaleDecorator<String>() {

            @Override
            public String executeDB() {
                throw new StaleModelException("test error");
            }
        };

        foo.executeNoTransaction("test error");

        Mockito.verify(mockAdapter, times(0)).beginTX();
        Mockito.verify(mockAdapter, times(0)).rollbackTX();
        Mockito.verify(mockAdapter, times(0)).commitTX();
        Mockito.verify(mockAdapter, times(RetryOnStaleDecorator.RETRY_COUNT_ON_STALED_DATA)).openConnection();
        Mockito.verify(mockAdapter, times(RetryOnStaleDecorator.RETRY_COUNT_ON_STALED_DATA)).closeConnection();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testBasicNoTransactionThrowsRuntimeException() {
        final ActiveJDBCAdapter mockAdapter = Mockito.mock(ActiveJDBCAdapter.class);
        TCSDriver.setDbAdapter(mockAdapter);

        final RetryOnStaleDecorator<String> foo = new RetryOnStaleDecorator<String>() {

            @Override
            public String executeDB() {
                throw new IllegalArgumentException("test error");
            }
        };

        foo.executeNoTransaction("test error");

        Mockito.verify(mockAdapter, times(0)).beginTX();
        Mockito.verify(mockAdapter, times(0)).rollbackTX();
        Mockito.verify(mockAdapter, times(0)).commitTX();
        Mockito.verify(mockAdapter, times(1)).openConnection();
        Mockito.verify(mockAdapter, times(1)).closeConnection();
    }
}
