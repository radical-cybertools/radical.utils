#!/usr/bin/env python

import os
import sys
import time
import numpy
import random
import pprint
import thread
import threading as mt

from   PyQt5 import QtWidgets
from   PyQt5 import QtCore
from   PyQt5 import QtGui

import radical.utils as ru

dh = ru.DebugHelper()

CORES     =  1024*1024   # number of cores to schedule over (sqrt must be int)
PPN       =    32        # cores per node, used for alignment
GPN       =     2        # GPUs per node, never aligned
          
ALIGN     =  True        # align small req onto single node
SCATTER   =  True        # allow scattered allocattions as fallback

CYCLES    = 10000        # number of cycles 
CPU_MIN   =     0        # minimal number of cores requested
CPU_MAX   =    16        # maximal number of cores requested
GPU_MIN   =     0        # minimal number of GPUs  requested
GPU_MAX   =     4        # maximal number of GPUs  requested
REQ_BULK  =  1024        # number of requests to handle in bulk
REL_PROB  =     0.010    # probablility of release per cycle

VIZ       = False        # show visualization

# ------------------------------------------------------------------------------
#
def drive_scheduler(scheduler, viz, term, lock, 
                    cycles, req_min, req_max, req_bulk, rel_prob):
        
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
        for cycle in range(cycles):

            if term.is_set():
                return

            if abort_cycles:
                break

            # we randomly request cores in a certain range
            requests = list()
            for _ in range(req_bulk):
                requests.append(random.randint(req_min,req_max))
        
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
                    if random.random() < rel_prob:
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
    
    # continuous stretches of #free/busy cores
    if False:
        print
        print '\ncores :  free :  busy'
        counts = set(stats['free_dist'].keys() + stats['busy_dist'].keys())
        for count in sorted(counts):
            print '%5d : %5s : %5s' % (count, 
                    stats['free_dist'].get(count, ''), 
                    stats['busy_dist'].get(count, ''))

    # distributions of free cores over nodes
    print
    print 'free : nodes'
    for i in sorted(stats['node_free'].keys()):
        print ' %3d : %5d' % (i, stats['node_free'].get(i, ''))

    print
    print 'total cores  : %9d' % stats['total']
    print '      free   : %9d' % stats['free']
    print '      busy   : %9d' % stats['busy']
    print '      alloc  : %9d' % total_alloc
    print '      align  : %9d' % total_align
    print '      scatter: %9d' % total_scatter
    print '      dealloc: %9d' % total_dealloc
    print '      runtime: %9.1fs'  % (total_stop - total_start)
    print '      ops/sec: %9.1f/s' % ((total_alloc + total_dealloc) / (total_stop - total_start))

    if False:
        idx = 0
        with open('cores', 'w') as f:
            node = '%5d : ' % idx
            for b in scheduler._cores:
                if not idx % ppn:
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
class MyViz(QtWidgets.QWidget):

    def __init__(self, scheduler, term, lock):

        QtWidgets.QWidget.__init__(self)

        QtWidgets.QShortcut(QtGui.QKeySequence("Esc"), self, self.close)

        self._scheduler = scheduler
        self._term      = term
        self._lock      = lock

        self._layout = self._scheduler.get_layout()
        self._rows   = self._layout['rows']
        self._cols   = self._layout['cols']
        self._size   = self._layout['cores']

        self.resize(self._rows+20, self._cols+20)
        self.horizontalLayout = QtWidgets.QHBoxLayout(self)
        self.horizontalLayout.setSpacing(0)
      # self.horizontalLayout.setMargin(0)
        self.scrollArea = QtWidgets.QScrollArea(self)
        self.scrollArea.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
        self.scrollArea.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
        self.scrollArea.setWidgetResizable(False)
        self.scrollAreaWidgetContents = QtWidgets.QWidget()
        self.scrollAreaWidgetContents.setGeometry(QtCore.QRect(0, 0, 416, 236))
        self.label = QtWidgets.QLabel(self.scrollAreaWidgetContents)
        self.label.setGeometry(QtCore.QRect(0, 0, 200, 100))
        self.scrollArea.setWidget(self.scrollAreaWidgetContents)
        self.horizontalLayout.addWidget(self.scrollArea)

        # SET UP IMAGE
        self.IM = QtGui.QImage(self._cols, self._rows, QtGui.QImage.Format_Indexed8)
        self.label.setGeometry(QtCore.QRect(0,0,self._cols,self._rows))
        self.scrollAreaWidgetContents.setGeometry(QtCore.QRect(0,0,self._cols,self._rows))
        
      # # SET UP RECURRING EVENTS
      # self.timer = QtCore.QTimer()
      # self.timer.start(0)
      # self.connect(self.timer, QtCore.SIGNAL('timeout()'), self.updateData) 

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

        if not VIZ:
            return

        while not self._term.is_set():

            with self._lock:
                vals = scheduler.get_map().unpack()

            for i in range(len(vals)):
                self._pmap.data[i] = vals[i] 

            self.updateData()


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


    if len(sys.argv) >= 3:

        if not '/' in __file__:
            path = '.'
        elif __file__:
            path = os.path.dirname(__file__)
        else:
            path = os.path.dirname(sys.argv[0])
        config = ru.read_json("%s/radical-utils-scheduler.json" % path)

        cluster_id  = sys.argv[1]
        workload_id = sys.argv[2]

        cluster  = config['cluster'][cluster_id]
        workload = config['workload'][workload_id]

        print 'using cluster: %s'  % pprint.pformat(cluster)
        print 'using workload: %s' % pprint.pformat(workload)

        cores   =  int(cluster['cores'])
        ppn     =  int(cluster['ppn'])
        align   = bool(cluster['align'])
        scatter = bool(cluster['scatter'])

        cycles   =   int(workload['cycles'])
        req_min  =   int(workload['req_min'])
        req_max  =   int(workload['req_max'])
        req_bulk =   int(workload['req_bulk'])
        rel_prob = float(workload['rel_prob'])

    else:
        cores   = CORES
        ppn     = PPN
        align   = ALIGN
        scatter = SCATTER

        cycles   = CYCLES
        req_min  = REQ_MIN
        req_max  = REQ_MAX
        req_bulk = REQ_BULK
        rel_prob = REL_PROB


    lock      = mt.RLock()
    term      = mt.Event()
    scheduler = ru.scheduler.BitarrayScheduler({'cores'   : cores, 
                                                'ppn'     : ppn, 
                                                'align'   : align, 
                                                'scatter' : scatter})
    app = QtWidgets.QApplication(sys.argv)
    viz = MyViz(scheduler, term, lock)
    thr = mt.Thread(target=drive_scheduler, 
            args=[scheduler, viz, term, lock, 
                  cycles, req_min, req_max, req_bulk, rel_prob])
    thr.start()

    app.exec_()
    thr.join()

#
# ------------------------------------------------------------------------------
	
