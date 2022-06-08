package org.apache.bookkeeper.client;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.lang.ArrayUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.*;

@RunWith(Parameterized.class)
public class BookKeeperTest {

    // test params
    private DigestType digestType;
    private byte[] passwd;
    private int ensSize;
    private int writeQuorumSize;
    private int ackQuorumSize;
    /** if null, we expect that a not null ledgeHandler instance is returned*/
    private Class<? extends Exception> exceptionExcpected;
    /*Set to true if you want to create a null handle after successful async creation of ledger */
    private boolean successfulCreationReturnsNullHandle;

    @Spy
    private BookKeeper client;
    @Mock
    private BookieWatcher bookieWatcher;
    @Spy
    private LedgerIdGenerator ledgerIdGenerator;
    @Spy
    private LedgerManager ledgerManager;
    @Mock
    private CompletableFuture<Versioned<LedgerMetadata>> whenComplete;
    @Mock
    private Versioned<LedgerMetadata> written;
    @Mock
    private LedgerMetadata metadata;
    private Map<String, MockedStatic<?>> mockedStatics = new HashMap<>();

    @Parameters(name = "{0}, {1}, {2}, {3}, {4}, {5}, {6}")
    public static Collection<Object[]> getParams(){
    return Arrays.asList(new Object[][]{
            // Firt iteration - Boundary Analysis
            { Exception.class,                      null,               ArrayUtils.toPrimitive(new Byte[]{1}),      1,   1,    1, false },
            { Exception.class,                      DigestType.DUMMY,   null,                                       1,   1,    1, false },
            { null,                                 DigestType.CRC32,   ArrayUtils.toPrimitive(new Byte[0]),        1,   1,    1, false },
            { null,                                 DigestType.MAC,     ArrayUtils.toPrimitive(new Byte[]{1}),      1,   1,    1, false },
            { null,                                 DigestType.MAC,     ArrayUtils.toPrimitive(new Byte[0]),        1,   1,    1, false },
            { Exception.class,                      DigestType.MAC,     null,                                       1,   1,    1, false },
            { Exception.class,                      DigestType.CRC32,   ArrayUtils.toPrimitive(new Byte[]{1}),     -1,   0,    0, false },
            { Exception.class,                      DigestType.CRC32,   ArrayUtils.toPrimitive(new Byte[]{1}),      0,  -1,    0, false },
            { Exception.class,                      DigestType.CRC32,   ArrayUtils.toPrimitive(new Byte[]{1}),      0,   0,   -1, false },
            { Exception.class,                      DigestType.CRC32,   ArrayUtils.toPrimitive(new Byte[]{1}),      0,   0,    1, false },
            { null,                                 DigestType.CRC32,   ArrayUtils.toPrimitive(new Byte[]{1}),      1,   1,    1, false },
            // Second Iteration - increment def-use and statement coverage
            { Exception.class,                      DigestType.CRC32,   ArrayUtils.toPrimitive(new Byte[0]),        1,   1,    1, true  },  // +2 p-use covered
            // Third iteration - improved mutation coverage
            { IllegalArgumentException.class,       DigestType.CRC32,   ArrayUtils.toPrimitive(new Byte[]{1}),      0,   0,    1, false },  // +1 mutants killed
        });
    }
    
    private void configure() throws 
    Exception{
        if (successfulCreationReturnsNullHandle){
            mockedStatics.put(SyncCallbackUtils.class.getName(), Mockito.mockStatic(SyncCallbackUtils.class));
            mockedStatics.get(SyncCallbackUtils.class.getName())
                    .when( () -> SyncCallbackUtils.waitForResult(any()) )
                    .thenReturn(null);
        }

        // mocking dependencies needed to execute the create ledger op
        MockitoAnnotations.initMocks(this);
        doAnswer(invocation -> {
                    ((BookkeeperInternalCallbacks.GenericCallback<Long>) invocation.getArguments()[0]).operationComplete(BKException.Code.OK, new Long(1234));
                    return  null;
                })
                .when(ledgerIdGenerator).generateLedgerId(any(BookkeeperInternalCallbacks.GenericCallback.class));
        when(bookieWatcher.newEnsemble(anyInt(), anyInt(), anyInt(), any())).thenReturn(Arrays.asList(BookieId.parse("test")));
        when(ledgerManager.createLedgerMetadata(anyLong(), any(LedgerMetadata.class))).thenReturn(whenComplete);
        when(written.getValue()).thenReturn(metadata);
        doAnswer( invocation -> {
            ((BiConsumer<Versioned<LedgerMetadata>, Throwable>) invocation.getArguments()[0]).accept(written, null);
            return null;
        }).when(whenComplete).whenComplete(any(BiConsumer.class));
        when(client.getLedgerIdGenerator()).thenReturn(ledgerIdGenerator);
        when(client.getBookieWatcher()).thenReturn(bookieWatcher);
        when(client.getLedgerManager()).thenReturn(ledgerManager);

    }
    
    public BookKeeperTest(Class<? extends Exception> exceptionExcpected, DigestType digestType, byte[] passwd, int ensSize, int writeQuorumSize, int ackQuorumSize, boolean successfulCreationReturnsNullHandle) throws Exception {
        this.digestType = digestType;
        this.passwd = passwd;
        this.ensSize = ensSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
        this.exceptionExcpected = exceptionExcpected;
        this.successfulCreationReturnsNullHandle = successfulCreationReturnsNullHandle;
        configure();
    }

    @Test
    public void testCreateLedger() {
        LedgerHandle handle;
        try {
            handle = client.createLedger(ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd);
            assertNotNull(handle);  // we assume any not null handle instance as valid
            assertNull(exceptionExcpected);
        } catch (Exception e){
            Logger.getGlobal().log(Level.INFO, e.getMessage(), e);
            assertTrue(exceptionExcpected.isAssignableFrom(e.getClass()));
        }
    }

    @After
    public void cleanup(){
        // close statick mocks
        mockedStatics.forEach( (n, ms) -> ms.close() );
    }
}
