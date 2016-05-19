#!/usr/bin/env python

import sys
import time
import numpy
import random
import threading as mt

from   PyQt4 import QtCore
from   PyQt4 import QtGui

import radical.utils as ru

CYCLES  = 3 
VERBOSE = True
VERBOSE = False
TEST    = True
TEST    = False

if TEST:
    ROWS      = 32              # for plotting
    COLUMNS   = 32              # for plotting
    CORES     = ROWS * COLUMNS  # total number of cores
    PPN       = 16              # cores per node
    
    REQ_MIN   = 1               # minimal number of cores requested
    REQ_MAX   = 16              # maximal number of cores requested
    REQ_STEP  = 1               # step size in request range above
    REQ_BULK  = 10              # number of requests to handle in bulk
    
    REL_PROB  = 0.200           # probablility of release per cycle
              
    ALIGN     = True            # small req on single node
    SCATTER   = True            # allow scattered as fallback

else:
    ROWS      = 1024            # for plotting
    COLUMNS   = 1024            # for plotting
    CORES     = ROWS * COLUMNS  # total number of cores
    CORES     = 1024*1024       # total number of cores
    COLUMNS   = 1024            # for plotting
    PPN       = 24              # cores per node
    
    REQ_MIN   = 1               # minimal number of cores requested
    REQ_MAX   = 10              # maximal number of cores requested
    REQ_STEP  = 1               # step size in request range above
    REQ_BULK  = 1024*16         # number of requests to handle in bulk
    
    REL_PROB  = 0.01            # probablility of release per cycle

    ALIGN     = True            # small req on single node
    SCATTER   = True            # allow scattered as fallback


# ------------------------------------------------------------------------------
#
def drive_scheduler(scheduler, viz):
        
    # leave some time for win mapping
    time.sleep(3)

    # ------------------------------------------------------------------------------
    #
    # This implementation will first create a number of requests, specifically
    # 'REQ_BULK' requests, and will allocate all of them, storing them in
    # a 'running' list.  After that requested list is allocated, all items in
    # 'running' are up for release, with a certain probaility REL_PROB.  Eg., for 
    # 'REL_PROB = 0.01', entries will be approx. be released after approx 100 
    # cycles.
    #
    # The above cycle repeats, but with a now (potentially) non-empty 
    # 'running' list to which the alloc portion will append.  The load on the
    # scheduler is thus continuously increasing as cores remain allocated over
    # cycles.
    #
    # This scheme repeats for CYCLES cycles.
    #
    # ------------------------------------------------------------------------------
    
    
    running = list()
    done    = list()

    alloc_total   = 0
    dealloc_total = 0
    
    while True:

        # for range 1024:
        #   find 1024 chunks of 16  cores
        #   free  512 chunks of  8 or 16 cores (random)
        for cycle in range(CYCLES):

            # we randomly request cores in a certain range
            requests = list()
            for _ in range(REQ_BULK):
                requests.append(random.randint(REQ_MIN,REQ_MAX))
        
            alloc_start = time.time()
            for req in requests:
                running.append(scheduler.alloc(req))
            alloc_stop = time.time()
            alloc_total += len(requests)
        
            # build a list of release candidates and, well, release them
            to_release = list()
            for idx in reversed(range(len(running))):
                if random.random() < REL_PROB:
                    to_release.append(running[idx])
                    del(running[idx])
        
            dealloc_start = time.time()
            for res in to_release:
                scheduler.dealloc(res)
                done.append(res)
            dealloc_stop = time.time()
            dealloc_total += len(to_release)

            if (alloc_stop == alloc_start):
                alloc_rate = -1
            else:
                alloc_rate   = len(requests)   / (alloc_stop   - alloc_start)

            if (dealloc_stop == dealloc_start):
                dealloc_rate = -1
            else:
                dealloc_rate = len(to_release) / (dealloc_stop - dealloc_start)

            print "%6d alloc (%8.1f/s)  %6d dealloc (%8.1f/s)  %6d free" % \
                    (alloc_total, alloc_rate, dealloc_total, dealloc_rate, scheduler._cores.count())

        

class MyViz(QtGui.QWidget):

    def __init__(self, scheduler):

        QtGui.QWidget.__init__(self)
        self._scheduler = scheduler

        self._layout = self._scheduler.get_layout()
        self._rows   = self._layout['rows']
        self._cols   = self._layout['cols']
        self._size   = self._layout['cores']

        self.resize(1024, 1024)
        self.horizontalLayout = QtGui.QHBoxLayout(self)
        self.horizontalLayout.setSpacing(0)
        self.horizontalLayout.setMargin(0)
        self.scrollArea = QtGui.QScrollArea(self)
        self.scrollArea.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOn)
        self.scrollArea.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOn)
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


    # --------------------------------------------------------------------------
    #
    def updateData(self):
    
        vals = scheduler.get_map().unpack()
        for i in range(len(vals)):
            self._pmap.data[i] = vals[i] 
    
        QI  = QtGui.QImage(self._pmap.data, self._cols, self._rows, QtGui.QImage.Format_Indexed8)    
        QI.setColorTable(self._colors)
        self.label.setPixmap(QtGui.QPixmap.fromImage(QI))    
    

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    scheduler = ru.scheduler.BitarrayScheduler({'cores'   : 1024*1024, 
                                                'ppn'     : 32, 
                                                'align'   : ALIGN, 
                                                'scatter' : SCATTER})
    app = QtGui.QApplication(sys.argv)
    viz = MyViz(scheduler)

    thr = mt.Thread(target=drive_scheduler, args=[scheduler, viz])
    thr.start()

    sys.exit(app.exec_())
	
