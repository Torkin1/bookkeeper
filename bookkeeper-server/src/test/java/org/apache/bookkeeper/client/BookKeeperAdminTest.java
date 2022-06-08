package org.apache.bookkeeper.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.bookkeeper.stats.StatsLogger;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.*;

@RunWith(Parameterized.class)
public class BookKeeperAdminTest {

    enum LedgerIdEquivalenceClass {
        REGISTERED,
        UNREGISTERED
        ;
    }

    /**set to -1 if an exception is expected */
    private int expectedNumOfEntries;
    private LedgerIdEquivalenceClass ledgerIdEquivalenceClass;
    private long ledgerId;
    private long firstEntry;
    private long lastEntry;

    /** SUT */
    @Spy
    @InjectMocks
    private BookKeeperAdmin bookKeeperAdmin;
    
    // mocks
    @Mock private BookKeeper bkc;
    @Mock private StatsLogger statsLogger;
    @Mock private LedgerHandle ledgerHandle;
    Enumeration<LedgerEntry> entries;
    private MockedStatic<SyncCallbackUtils> syncCallbackUtils;

    @Parameters(name = "{0}, {1}, {2}, {3}, {4}")
    public static Collection<Object[]> getParams(){
        return Arrays.asList(new Object[][]{
            { -1, LedgerIdEquivalenceClass.UNREGISTERED,     0,  0,  0 },
            { -1, LedgerIdEquivalenceClass.REGISTERED,       0, -1,  0 },
            {  0, LedgerIdEquivalenceClass.REGISTERED,       0,  0, -2 },
            {  1, LedgerIdEquivalenceClass.REGISTERED,       0,  0,  0 },
            {  2, LedgerIdEquivalenceClass.REGISTERED,       0,  0,  1 },
            {  0, LedgerIdEquivalenceClass.REGISTERED,       0,  1,  0 },
            {  3, LedgerIdEquivalenceClass.REGISTERED,       0,  0, -1 }
        });
    }
        
    private void configure() throws BKException, InterruptedException{

        MockitoAnnotations.initMocks(this);
        syncCallbackUtils = Mockito.mockStatic(SyncCallbackUtils.class);
        if(ledgerIdEquivalenceClass.equals(LedgerIdEquivalenceClass.REGISTERED)){

            // a generator of dummy ledgerEntry objects
            entries = new Enumeration<LedgerEntry>() {
                private int numOfFetches = 0;

                @Override
                public boolean hasMoreElements() {
                    return numOfFetches < expectedNumOfEntries;
                }

                @Override
                public LedgerEntry nextElement() {
                    if (numOfFetches >= expectedNumOfEntries - 1){
                        syncCallbackUtils.when(
                                () -> SyncCallbackUtils.waitForResult(any())
                        ).thenThrow(new BKException.BKNoSuchEntryException());
                    }
                    numOfFetches ++;
                    return Mockito.mock(LedgerEntry.class);
                }
            };
            // mock fetch of ledgerEntry objects
            doReturn(ledgerHandle).when(bookKeeperAdmin).openLedgerNoRecovery(anyLong());
            doAnswer(
                    invocation -> null
            ).when(ledgerHandle).asyncReadEntriesInternal(anyLong(), anyLong(), any(), any(), anyBoolean());
            syncCallbackUtils.when(
                    () -> SyncCallbackUtils.waitForResult(any())
            ).thenReturn(entries);

        }
        else {
            // simulation of orphan ledger id
            Exception e = new BKException.BKNoSuchLedgerExistsException();
            doThrow(e).when(bookKeeperAdmin).openLedgerNoRecovery(anyLong());
        }
    }
    
    public BookKeeperAdminTest(int expectedNumOfEntries, LedgerIdEquivalenceClass ledgerIdEquivalenceClass,  long ledgerId, long firstEntry, long lastEntry) throws BKException, IOException, InterruptedException {
        this.lastEntry = lastEntry;
        this.expectedNumOfEntries = expectedNumOfEntries;
        this.ledgerIdEquivalenceClass = ledgerIdEquivalenceClass;
        this.ledgerId = ledgerId;
        this.firstEntry = firstEntry;
        configure();
    }

    @Test
    public void testReadEntries() {
        try {
            int numOfEntries = 0;
            Iterable<LedgerEntry> entries = bookKeeperAdmin.readEntries(ledgerId, firstEntry, lastEntry);
            Iterator<LedgerEntry> iterator = entries.iterator();
            while (iterator.hasNext()){
                iterator.next();
                numOfEntries ++;
            }
            iterator.hasNext(); // +2 p-use
            assertEquals(expectedNumOfEntries, numOfEntries);
        } catch (Exception e) {
            Logger.getGlobal().log(Level.INFO, e.getMessage(), e);
            assertTrue(expectedNumOfEntries < 0);
        }
        
    }

    @After
    public  void clean(){

        // close all static mocks
        syncCallbackUtils.close();
    }
}
