/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cutlass.line.tcp;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.Sequence;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectCharSink;
import io.questdb.std.str.FloatingDirectCharSink;
import io.questdb.std.str.Path;

import static io.questdb.cutlass.line.tcp.LineTcpMeasurementEvent.RELEASE_WRITER_EVENT_ID;
import static io.questdb.cutlass.line.tcp.LineTcpMeasurementEvent.RESHUFFLE_EVENT_ID;

class WriterJob implements Job {
    private static final Log LOG = LogFactory.getLog(WriterJob.class);

    private final LineTcpMeasurementScheduler lineTcpMeasurementScheduler;
    private final int workerId;
    private final Sequence sequence;
    private final Path path = new Path();
    private final DirectCharSink charSink = new DirectCharSink(64);
    private final FloatingDirectCharSink floatingCharSink = new FloatingDirectCharSink();
    private final ObjList<LineTcpMeasurementScheduler.TableUpdateDetails> assignedTables = new ObjList<>();
    private long lastMaintenanceMillis = 0;

    WriterJob(LineTcpMeasurementScheduler lineTcpMeasurementScheduler, int id, Sequence sequence) {
        super();
        this.lineTcpMeasurementScheduler = lineTcpMeasurementScheduler;
        this.workerId = id;
        this.sequence = sequence;
    }

    DirectCharSink getCharSink() {
        return charSink;
    }

    FloatingDirectCharSink getFloatingCharSink() {
        return floatingCharSink;
    }

    @Override
    public boolean run(int workerId) {
        assert this.workerId == workerId;
        boolean busy = drainQueue();
        doMaintenance();
        return busy;
    }

    void close() {
        LOG.info().$("line protocol writer closing [threadId=").$(workerId).$(']').$();
        // Finish all jobs in the queue before stopping
        for (int n = 0; n < lineTcpMeasurementScheduler.queue.getCycle(); n++) {
            if (!run(workerId)) {
                break;
            }
        }

        Misc.free(path);
        Misc.free(charSink);
        Misc.free(floatingCharSink);
        Misc.freeObjList(assignedTables);
        assignedTables.clear();
    }

    private void doMaintenance() {
        final long millis = lineTcpMeasurementScheduler.milliClock.getTicks();
        if (millis - lastMaintenanceMillis < lineTcpMeasurementScheduler.maintenanceInterval) {
            return;
        }

        lastMaintenanceMillis = millis;
        for (int n = 0, sz = assignedTables.size(); n < sz; n++) {
            assignedTables.getQuick(n).handleWriterThreadMaintenance(millis);
        }
    }

    private boolean drainQueue() {
        boolean busy = false;
        while (true) {
            long cursor;
            while ((cursor = sequence.next()) < 0) {
                if (cursor == -1) {
                    return busy;
                }
            }
            busy = true;
            final LineTcpMeasurementEvent event = lineTcpMeasurementScheduler.queue.get(cursor);
            boolean eventProcessed;

            try {
                if (event.getThreadId() == workerId) {
                    try {
                        if (!event.tableUpdateDetails.isAssignedToJob()) {
                            assignedTables.add(event.tableUpdateDetails);
                            event.tableUpdateDetails.setAssignedToJob(true);
                            LOG.info().$("assigned table to writer thread [tableName=").$(event.tableUpdateDetails.tableName).$(", threadId=").$(workerId).I$();
                        }
                        event.processMeasurementEvent(this);
                        eventProcessed = true;
                    } catch (Throwable ex) {
                        LOG.error().$("closing writer for because of error [table=").$(event.tableUpdateDetails.tableName).$(",ex=").$(ex).I$();
                        event.createWriterReleaseEvent(event.tableUpdateDetails, false);
                        eventProcessed = false;
                    }
                } else {
                    switch (event.getThreadId()) {
                        case RESHUFFLE_EVENT_ID:
                            eventProcessed = processReshuffleEvent(event);
                            break;

                        case RELEASE_WRITER_EVENT_ID:
                            eventProcessed = processWriterReleaseEvent(event);
                            break;

                        default:
                            eventProcessed = true;
                            break;
                    }
                }
            } catch (Throwable ex) {
                eventProcessed = true;
                LOG.error().$("failed to process ILP event because of exception [ex=").$(ex).I$();
            }

            // by not releasing cursor we force the sequence to return us the same value over and over
            // until cursor value is released
            if (eventProcessed) {
                sequence.done(cursor);
            } else {
                return false;
            }
        }
    }

    private boolean processReshuffleEvent(LineTcpMeasurementEvent event) {
        if (event.rebalanceToThreadId == workerId) {
            // This thread is now a declared owner of the table, but it can only become actual
            // owner when "old" owner is fully done. This is a volatile variable on the event, used by both threads
            // to handover the table. The starting point is "false" and the "old" owner thread will eventually set this
            // to "true". In the mean time current thread will not be processing the queue until the handover is
            // complete
            if (event.rebalanceReleasedByFromThread) {
                LOG.info().$("rebalance cycle, new thread ready [threadId=").$(workerId).$(", table=").$(event.tableUpdateDetails.tableName).$(']').$();
                return true;
            }

            return false;
        }

        if (event.rebalanceFromThreadId == workerId) {
            for (int n = 0, sz = assignedTables.size(); n < sz; n++) {
                if (assignedTables.get(n) == event.tableUpdateDetails) {
                    assignedTables.remove(n);
                    break;
                }
            }
            LOG.info()
                    .$("rebalance cycle, old thread finished [threadId=").$(workerId)
                    .$(", table=").$(event.tableUpdateDetails.tableName)
                    .I$();
            event.tableUpdateDetails.setAssignedToJob(false);
            event.rebalanceReleasedByFromThread = true;
        }

        return true;
    }

    private boolean processWriterReleaseEvent(LineTcpMeasurementEvent event) {
        lineTcpMeasurementScheduler.tableUpdateDetailsLock.readLock().lock();
        try {
            if (event.tableUpdateDetails.getWriterThreadId() != workerId) {
                return true;
            }
            final LineTcpMeasurementScheduler.TableUpdateDetails tableUpdateDetails = event.tableUpdateDetails;
            if (lineTcpMeasurementScheduler.tableUpdateDetailsByTableName.keyIndex(tableUpdateDetails.tableName) < 0) {
                // Table must have been re-assigned to an IO thread
                return true;
            }
            LOG.info()
                    .$("releasing writer, its been idle since ").$ts(tableUpdateDetails.getLastMeasurementMillis() * 1_000)
                    .$("[tableName=").$(tableUpdateDetails.tableName)
                    .I$();

            tableUpdateDetails.handleWriterRelease(event.isCommitOnWriterClose());
        } finally {
            lineTcpMeasurementScheduler.tableUpdateDetailsLock.readLock().unlock();
        }
        return true;
    }
}
