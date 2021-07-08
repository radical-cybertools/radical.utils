#!/usr/bin/env python3

import os
import sys
import time
import random
import pprint
import _thread

import radical.utils as ru


try:
    # make pylint happy - this is optional code
    from Xlib import X, display, Xutil
except ImportError as exc:
    raise RuntimeError('need Xlib module to work') from exc



ROWS      =  1024
COLS      =  1024
CORES     =  ROWS * COLS  # number of cores to schedule over (sqrt must be int)
PPN       =    32         # cores per node, used for alignment
GPN       =     2         # GPUs per node, never aligned

ALIGN     =  True         # align small req onto single node
SCATTER   =  True         # allow scattered allocattions as fallback

CYCLES    = 10000         # number of cycles
CPU_MIN   =     0         # minimal number of cores requested
CPU_MAX   =    32         # maximal number of cores requested
GPU_MIN   =     0         # minimal number of GPUs  requested
GPU_MAX   =     4         # maximal number of GPUs  requested
REQ_BULK  =     2         # number of requests to handle in bulk
REL_PROB  =     0.010     # probablility of release per cycle
REQ_MIN   =     1         # probablility of release per cycle
REQ_MAX   =     2         # probablility of release per cycle


# ------------------------------------------------------------------------------
#
class SchedulerViz(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, args):

        self.scheduler = args[0]
        self.cycles    = args[1]
        self.req_min   = args[2]
        self.req_max   = args[3]
        self.req_bulk  = args[4]
        self.rel_prob  = args[5]

        print(scheduler)

        self.d      = display.Display()
        self.screen = self.d.screen()
        self.window = self.screen.root.create_window(
            0, 0, ROWS, COLS, 2,
            self.screen.root_depth,
            X.InputOutput,
            X.CopyFromParent,

            background_pixel=self.screen.white_pixel,

            event_mask=(X.ExposureMask        |
                        X.StructureNotifyMask |
                        X.ButtonPressMask     |
                        X.ButtonReleaseMask   |
                        X.Button1MotionMask   ),
            colormap=X.CopyFromParent)

        self.gc = self.window.create_gc(foreground=self.screen.black_pixel,
                                        background=self.screen.white_pixel)

        self.WM_DELETE_WINDOW = self.d.intern_atom('WM_DELETE_WINDOW')
        self.WM_PROTOCOLS     = self.d.intern_atom('WM_PROTOCOLS')

        self.window.set_wm_name     ('ru_scheduler viz')
        self.window.set_wm_icon_name('ru_scheduler')
        self.window.set_wm_class    ('ru_scheduler', 'XlibExample')

        self.window.set_wm_protocols([self.WM_DELETE_WINDOW])
        self.window.set_wm_hints(flags=Xutil.StateHint,
                                 initial_state=Xutil.NormalState)

        self.window.set_wm_normal_hints(flags=(Xutil.PPosition | Xutil.PSize |
                                               Xutil.PMinSize),
                                        min_width=20,
                                        min_height=20)
        self.window.map()

        self._thread = _thread.start_new_thread(self.update_viz,   tuple())
        self._thread = _thread.start_new_thread(self.update_sched, tuple())


    # --------------------------------------------------------------------------
    # Main loop, handling events
    def loop(self):

        while 1:
            e = self.d.next_event()

            if e.type == X.DestroyNotify:
              # self.term.set()
                sys.exit(0)

            if e.type == X.ButtonPress and e.detail == 1:
              # self.term.set()
                sys.exit(0)

            if e.type == X.ClientMessage:
                if e.client_type == self.WM_PROTOCOLS:
                    fmt, data = e.data
                    if fmt == 32 and data[0] == self.WM_DELETE_WINDOW:
                      # self.term.set()
                        sys.exit(0)


    # --------------------------------------------------------------------------
    #
    def update_viz(self):

        # max len for point list us USHORT_MAX, ie. 64k
        chunk = 128 * 128
        cmap  = self.d.screen().default_colormap
        red   = cmap.alloc_named_color("red" ).pixel
        blue  = cmap.alloc_named_color("blue").pixel

        while True:

            print(1)

            state   = self.scheduler.get_map()
            active  = list()
            passive = list()

            active_chunk  = list()
            passive_chunk = list()

            for r in range(ROWS):
                for c in range(COLS):
                    if state[r * COLS + c]:
                        active_chunk.append((r, c))
                        if len(active_chunk) > chunk:
                            active.append(active_chunk)
                            active_chunk = list()
                    else:
                        passive_chunk.append((r, c))
                        if len(passive_chunk) > chunk:
                            passive.append(passive_chunk)
                            passive_chunk = list()

            if active_chunk:
                active.append(active_chunk)
            if passive_chunk:
                passive.append(passive_chunk)

            print(2)

            self.gc.change(foreground=red)
            for active_chunk in active:
                self.window.poly_point(self.gc, X.CoordModeOrigin, active_chunk)

            self.gc.change(foreground=blue)
            for passive_chunk in passive:
                self.window.poly_point(self.gc, X.CoordModeOrigin,passive_chunk)
            print(3)

            self.d.flush()
            print('done')
            time.sleep(0.1)


    # --------------------------------------------------------------------------
    #
    def update_sched(self):

        try:
            # ------------------------------------------------------------------
            #
            # This implementation will first create a number of requests,
            # specifically 'REQ_BULK' requests, each randomly distributed
            # between 'REQ_MIN' and 'REQ_MAX'.  Those requests are the
            # scheduled via calls to 'self.scheduler.allocate(req)', and the
            # results are stored in a 'running' list.
            #
            # After that requested bulk is allocated, all items in 'running'
            # (including those allocated in earlier self.cycles)  are up for
            # release, with a certain probaility REL_PROB.  For example, for
            # 'REL_PROB = 0.01',  approximately 1% of the entries will be
            # released, via a call to 'self.scheduler.deallocate(res)'.
            #
            # The above cycle repeats 'self.cycles' times, with a (potentially)
            # ever increasing 'running' list, The load on the self.scheduler is
            # thus continuously increasing as cores remain allocated over
            # self.cycles, and the self.scheduler needs to search harder for
            # free cores
            #
            # This thread finishes when:
            #   - all self.cycles are completed
            #   - the self.scheduler cannot find anough cores for a request
            #   - the self.scheduler can't align an allocation (if 'ALIGN' is
            #     set)
            #
            # ------------------------------------------------------------------

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
            for cycle in range(self.cycles):

                time.sleep(0.1)

                if abort_cycles:
                    break

                # we randomly request cores in a certain range
                requests = list()
                for _ in range(self.req_bulk):
                    requests.append(random.randint(self.req_min,self.req_max))

                tmp = list()
                start = time.time()
                try:
                    for req in requests:
                        tmp.append(self.scheduler.alloc(req))
                except Exception as e:
                    print(e)
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
                    for idx in reversed(list(range(len(running)))):
                        if random.random() < self.rel_prob:
                            to_release.append(running[idx])
                            del(running[idx])

                    start = time.time()
                    try:
                        for res in to_release:
                            self.scheduler.dealloc(res)
                            done.append(res)
                    except Exception as e:
                        print(e)
                        abort_cycles = True
                    stop   = time.time()
                    total_dealloc += len(to_release)

                    if (stop == start):
                        dealloc_rate = -1
                    else:
                        dealloc_rate = len(to_release) / (stop - start)

                print('%5d : alloc : %6d (%8.1f/s)   dealloc : %6d (%8.1f/s)'
                      'free %6d' % (cycle, total_alloc,   alloc_rate,
                                           total_dealloc, dealloc_rate,
                                           self.scheduler.get_map.count()))

            if abort_cycles:
                print('cycle aborted')
            else:
                print('cycles done')

        except Exception:
            import traceback
            print(traceback.format_exc(sys.exc_info()))

        total_stop = time.time()
        stats = self.scheduler.get_stats()

        # NOTE: Uncomment to DEBUG
        # continuous stretches of #free/busy cores
        # print()
        # print('\ncores :  free :  busy')
        # counts = set(list(stats['free_dist'].keys()) + \
        #          list(stats['busy_dist'].keys()))
        # for count in sorted(counts):
        #     print('%5d : %5s : %5s' % (count,
        #             stats['free_dist'].get(count, ''),
        #             stats['busy_dist'].get(count, '')))

        # distributions of free cores over nodes
        print()
        print('free : nodes')
        for i in sorted(stats['node_free'].keys()):
            print(' %3d : %5d' % (i, stats['node_free'].get(i, '')))

        print()
        print('total cores  : %9d' % stats['total'])
        print('      free   : %9d' % stats['free'])
        print('      busy   : %9d' % stats['busy'])
        print('      alloc  : %9d' % total_alloc)
        print('      align  : %9d' % total_align)
        print('      scatter: %9d' % total_scatter)
        print('      dealloc: %9d' % total_dealloc)
        print('      runtime: %9.1fs'  % (total_stop   - total_start))
        print('      ops/sec: %9.1f/s' % ((total_alloc + total_dealloc) /
                                          (total_stop  - total_start)))

        # NOTE: Uncomment to DEBUG
        # idx = 0
        # with open('cores', 'w') as f:
        #     node = '%5d : ' % idx
        #     for b in self.scheduler.get_map():
        #         if not idx % ppn:
        #             node += '\n'
        #         if b:
        #             node += '#'
        #         else:
        #             node += ' '
        #         idx += 1
        #     f.write(node)

        print('\nuse <Esc> in viz-window to quit\n')


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    if len(sys.argv) >= 3:

        if '/' not in __file__:
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

        print('using cluster: %s'  % pprint.pformat(cluster))
        print('using workload: %s' % pprint.pformat(workload))

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


    scheduler = ru.scheduler.BitarrayScheduler({'cores'   : cores,
                                                'ppn'     : ppn,
                                                'align'   : align,
                                                'scatter' : scatter})

    vs_args = [scheduler, cycles, req_min, req_max, req_bulk, rel_prob]
    vs      = SchedulerViz(vs_args)
    vs.loop()


# ------------------------------------------------------------------------------

