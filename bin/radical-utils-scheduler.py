#!/usr/bin/env python

import sys
import time
import numpy
import random
import thread
import threading as mt

from   PyQt4 import QtCore
from   PyQt4 import QtGui

import radical.utils as ru

dh = ru.DebugHelper()

CORES   = 1024*1024  # number of cores to schedule over (sqrt must be int)
PPN     = 32         # cores per node, used for alignment
        
ALIGN   = True       # align small req onto single node
SCATTER = True       # allow scattered allocattions as fallback

# ------------------------------------------------------------------------------
#
def drive_scheduler(scheduler, viz, term, lock):
        
    try:
        # leave some time for win mapping
        time.sleep(1)

        # ----------------------------------------------------------------------
        #
        # This implementation will first create a number of requests,
        # specifically 'REQ_BULK' requests, each randomly distributed between
        # 'REQ_MIN' and 'REQ_MAX'.  Those requests are the scheduled via calls
        # to 'scheduler.allocate(req)', and the results are stored in
        # a 'running' list.
        #
        # After that requested bulk is allocated, all items in 'running'
        # (including those allocated in earlier cycles)  are up for release,
        # with a certain probaility REL_PROB.  For example, for 'REL_PROB
        # = 0.01',  approximately 1% of the entries will be released, via a call
        # to 'scheduler.deallocate(res)'.
        #
        # The above cycle repeats 'CYCLES' times, with a (potentially) ever
        # increasing 'running' list, The load on the scheduler is thus
        # continuously increasing as cores remain allocated over cycles, and the
        # scheduler needs to search harder for free cores
        #
        # This thread finishes when:
        #   - all cycles are completed
        #   - the scheduler cannot find anough cores for a request
        #   - the scheduler can't align an allocation (if 'ALIGN' is set)
        #
        # ----------------------------------------------------------------------
        
        CYCLES    = 1024            # number of cycles 
        
        REQ_MIN   =    1            # minimal number of cores requested
        REQ_MAX   =   64            # maximal number of cores requested
        REQ_BULK  = 1024            # number of requests to handle in bulk
        REL_PROB  = 0.011           # probablility of release per cycle

        # ----------------------------------------------------------------------

        running = list()
        done    = list()

        total_start   = time.time()
        total_alloc   = 0
        total_dealloc = 0
        total_align   = 0
        total_scatter = 0

        # for range 1024:
        #   find 1024 chunks of 16  cores
        #   free  512 chunks of  8 or 16 cores (random)
        abort_cycles = False
        for cycle in range(CYCLES):

            if term.is_set():
                return

            if abort_cycles:
                break

            # we randomly request cores in a certain range
            requests = list()
            for _ in range(REQ_BULK):
                requests.append(random.randint(REQ_MIN,REQ_MAX))
        
            tmp = list()
            with lock:
                start = time.time()
                try:
                    for req in requests:
                        tmp.append(scheduler.alloc(req))
                except Exception as e:
                    print e
                    abort_cycles = True
                stop = time.time()

            for res in tmp:
                total_alloc += 1
                if res[2]:
                    total_scatter += 1
                if res[3]:
                    total_align += 1
            running += tmp

            if (stop == start):
                alloc_rate = -1
            else:
                alloc_rate = len(requests) / (stop - start)

            if abort_cycles:
                # don't dealloc, as it screws with statistics
                dealloc_rate = -1
                
            else:
        
                # build a list of release candidates and, well, release them
                to_release = list()
                for idx in reversed(range(len(running))):
                    if random.random() < REL_PROB:
                        to_release.append(running[idx])
                        del(running[idx])
        
                with lock:
                    start = time.time()
                    try:
                        for res in to_release:
                            scheduler.dealloc(res)
                            done.append(res)
                    except Exception as e:
                        print e
                        abort_cycles = True
                    stop   = time.time()
                    total_dealloc += len(to_release)

                if (stop == start):
                    dealloc_rate = -1
                else:
                    dealloc_rate = len(to_release) / (stop - start)

            print "%5d : alloc : %6d (%8.1f/s)   dealloc : %6d (%8.1f/s)   free %6d" % \
                    (cycle, total_alloc, alloc_rate, 
                            total_dealloc, dealloc_rate, 
                            scheduler._cores.count())

        if abort_cycles:
            print 'cycle aborted'
        else:
            print 'cycles done'

    except Exception as e:
        import traceback
        print traceback.format_exc(sys.exc_info())
   
    total_stop = time.time()
    stats = scheduler.get_stats()
    
    print
    print '\ncores :  free :  busy'
    counts = set(stats['free_dist'].keys() + stats['busy_dist'].keys())
    for count in sorted(counts):
        print '%5d : %5s : %5s' % (count, 
                stats['free_dist'].get(count, ''), 
                stats['busy_dist'].get(count, ''))

    print
    print 'free : nodes'
    for i in sorted(stats['node_free'].keys()):
        print ' %3d : %5d' % (i, stats['node_free'].get(i, ''))

    print
    print 'total cores  : %7d' % stats['total']
    print '      free   : %7d' % stats['free']
    print '      busy   : %7d' % stats['busy']
    print '      alloc  : %7d' % total_alloc
    print '      align  : %7d' % total_align
    print '      scatter: %7d' % total_scatter
    print '      dealloc: %7d' % total_dealloc
    print '      runtime: %7.1fs'  % (total_stop - total_start)
    print '      ops/sec: %7.1f/s' % ((total_alloc + total_dealloc) / (total_stop - total_start))

    if True:
        idx = 0
        with open('cores', 'w') as f:
            node = '%5d : ' % idx
            for b in scheduler._cores:
                if not idx % PPN:
                    node += '\n'
                if b:
                    node += '#'
                else:
                    node += ' '
                idx += 1
            f.write(node)

    print '\nuse <Esc> in viz-window to quit\n'


# ------------------------------------------------------------------------------
#
class MyViz(QtGui.QWidget):

    def __init__(self, scheduler, term, lock):

        QtGui.QWidget.__init__(self)

        QtGui.QShortcut(QtGui.QKeySequence("Esc"), self, self.close)

        self._scheduler = scheduler
        self._term      = term
        self._lock      = lock

        self._layout = self._scheduler.get_layout()
        self._rows   = self._layout['rows']
        self._cols   = self._layout['cols']
        self._size   = self._layout['cores']

        self.resize(self._rows+5, self._cols+5)
        self.horizontalLayout = QtGui.QHBoxLayout(self)
        self.horizontalLayout.setSpacing(0)
        self.horizontalLayout.setMargin(0)
        self.scrollArea = QtGui.QScrollArea(self)
        self.scrollArea.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
        self.scrollArea.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
        self.scrollArea.setWidgetResizable(False)
        self.scrollAreaWidgetContents = QtGui.QWidget()
        self.scrollAreaWidgetContents.setGeometry(QtCore.QRect(0, 0, 416, 236))
        self.label = QtGui.QLabel(self.scrollAreaWidgetContents)
        self.label.setGeometry(QtCore.QRect(0, 0, 200, 100))
        self.scrollArea.setWidget(self.scrollAreaWidgetContents)
        self.horizontalLayout.addWidget(self.scrollArea)

        # SET UP IMAGE
        self.IM = QtGui.QImage(self._cols, self._rows, QtGui.QImage.Format_Indexed8)
        self.label.setGeometry(QtCore.QRect(0,0,self._cols,self._rows))
        self.scrollAreaWidgetContents.setGeometry(QtCore.QRect(0,0,self._cols,self._rows))
        
        # SET UP RECURRING EVENTS
        self.timer = QtCore.QTimer()
        self.timer.start(0)
        self.connect(self.timer, QtCore.SIGNAL('timeout()'), self.updateData) 

        self._pmap = numpy.ndarray([self._rows, self._cols])
        self._pmap = numpy.require(self._pmap, numpy.uint8, 'C')  # continuous memory layout 
        self._colors = [QtGui.qRgb(i,0,0) for i in range(256)]

        ### DISPLAY WINDOWS
        self.show()

        self._thr = mt.Thread(target=self.time_updates)
        self._thr.start()

    

    # --------------------------------------------------------------------------
    #
    def close(self):

        self._term.set()

    

    # --------------------------------------------------------------------------
    #
    def __del__(self):

        self._term.set()


    # --------------------------------------------------------------------------
    #
    def time_updates(self):

        while not self._term.is_set():
          # time.sleep(0.01)
            with self._lock:
                vals = scheduler.get_map().unpack()

            for i in range(len(vals)):
                self._pmap.data[i] = vals[i] 


    # --------------------------------------------------------------------------
    #
    def updateData(self):

        if self._term.is_set():
            sys.exit()

        QI = QtGui.QImage(self._pmap.data, self._cols, self._rows, 
             QtGui.QImage.Format_Indexed8)    
        QI.setColorTable(self._colors)
        self.label.setPixmap(QtGui.QPixmap.fromImage(QI))    


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    lock      = mt.RLock()
    term      = mt.Event()
    scheduler = ru.scheduler.BitarrayScheduler({'cores'   : CORES, 
                                                'ppn'     : PPN, 
                                                'align'   : ALIGN, 
                                                'scatter' : SCATTER})
    app = QtGui.QApplication(sys.argv)
    viz = MyViz(scheduler, term, lock)
    thr = mt.Thread(target=drive_scheduler, args=[scheduler, viz, term, lock])
    thr.start()

    app.exec_()
    thr.join()

#
# ------------------------------------------------------------------------------
	
