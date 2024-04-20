'''
Implements a file system watcher.

See also:

- https://inotify-simple.readthedocs.io/en/latest/#gracefully-exit-a-blocking-read
'''
#import fnmatch
import os
import time
import select
import logging
from pathlib import Path
from collections import defaultdict

from inotify_simple import INotify, Event as iEvent, flags as iflags, masks as imasks

from execlog import util 
from execlog.listener import Listener


logger = logging.getLogger(__name__)

# hardcoded file names to ignore
# - "4913" is a temp file created by Vim before editing
IGNORE_PATTERNS = ['4913', '.sync*.db*']

class PathListener(Listener):
    def __init__(self, router):
        '''
        Parameters:
            workers: number of workers to assign the thread pool when the event loop is
                     started. Defaults to `None`, which, when passed to
                     ThreadPoolExecutor, will by default use 5x the number of available
                     processors on the machine (which the docs claim is a reasonable
                     assumption given threads are more commonly leveraged for I/O work
                     rather than intense CPU operations). Given the intended context for
                     this class, this assumption aligns appropriately.

        Note:
            Due to the nature of INotify, you cannot watch the same path with two
            separate flag settings (without creating a new INotify instance). Under the
            same instance, calling `add_watch` for an already watched path location will
            simply return the watch descriptor already associated with that location (and
            may update the flags to whatever is passed). However, this location will only
            ever be "watched once" by a given INotify instance, so keep this in mind if
            multiple downstream callbacks (packaged up by the Router) want to react to the
            same path but with different flags (i.e., it won't work as expected).
        '''
        super().__init__(router)

        self.started = False

        self.pathmap  : dict[str, int] = {}
        self.canonmap : dict[int, tuple] = {}
        self.watchmap : dict[int, dict[tuple, int]] = defaultdict(lambda: defaultdict(int))

        self.unmatched_move_froms : dict[int, dict[str, str]] = defaultdict(dict)

        self.inotify = INotify()

        self.read_fd, write_fd = os.pipe()
        self.write = os.fdopen(write_fd, "wb")

    def _add_watch(
        self,
        path,
        flags,
        lead=None,
        remove=False,
    ):
        if lead is None: lead = ''
        path = Path(path)
        lead = Path(lead)

        wd = None
        fullpath = Path(path, lead)
        try:
            wd = self.inotify.add_watch(str(fullpath), flags)
            self.watchmap[wd][(path, lead)] |= flags
            self.pathmap[fullpath] = wd
        except FileNotFoundError:
            logger.error(f'Directory not found for [{fullpath}] when attempting to watch')

        return wd

    def _add_watch_recursive(
        self,
        path,
        flags,
        lead=None,
        remove=False,
    ):
        '''
        Recursively watch directories under path/lead, using `path` as the registered
        base. Specifying `lead` gives one control over the subdirectory on which to
        recurse; the "origin" the recursion base point.

        Note: on renamed/moved directories
            This method is used to reset the mapped path/lead tuples pointed to by certain
            watch descriptors when a directory is renamed. iNotify will fire a MOVED event
            for the directory (MOVED_FROM/MOVED_TO), but will use the same watch
            descriptor for the new directory as it did the old. This will leave all watch
            dynamics intact, but new file events will fire and send the old base path to
            the router. Explicitly re-watching the renamed directory (and any
            subdirectories) will also return that existing watch descriptor. Thus, this
            method can just be directly called for directory moves/renames, and WDs in the
            `watchmap` will just be used as expected. (One should also call the method
            using the old lead and set `remove=True` to remove old tuples out of the
            `watchmap`. Note that this doesn't remove the existing watches from iNotify,
            just their tracked tuples.)
        '''
        if lead is None:
            lead = ''

        wds = []
        origin = Path(path, lead)
        for subdir in [origin, *util.path.iter_glob_paths('**/', origin)]:
            lead = subdir.relative_to(Path(path))
            wd = self._add_watch(path, flags, lead=lead, remove=remove)
            if wd is not None:
                wds.append(wd)
        return wds

    def listen(
        self,
        path,
        flags=None,
    ):
        '''
        Parameters:
            path:  Path (directory) to watch with `inotify`
            flags: inotify_simple flags matching FS event types allowed to trigger the
                   callback
            debounce: time in milliseconds to debounce file-based events at this path.
                      Applies to _file targets_; the same filename will have events
                      "debounced" at this interval (time delta calculated from last
                      un-rejected match).
        '''
        path = Path(path)

        if flags is None:
            flags = iflags.CREATE | iflags.DELETE | iflags.MODIFY | iflags.DELETE_SELF | iflags.MOVED_TO

        # ensure flags can detect directory events
        flags |= iflags.CREATE | iflags.MOVED_FROM | iflags.MOVED_TO

        wds = self._add_watch_recursive(path, flags)

        try:
            self.canonmap[wds[0]] = (path, flags)
        except IndexError:
            logger.error(f'Path {path} returned no INotify watch descriptors')
            raise

    def run(self):
        '''
        Note: On usage
            `start()` is a blocking call. This will hog your main thread if not properly
            threaded. If handling this manually in your outer context, you will also need
            to make sure to call `.stop()`

        Parameters:
            loop: asyncio loop to pass to `_event_loop`; used to schedule async callbacks
                  when present
        '''
        self.started = True
        logger.info(f'Starting listener for {len(self.watchmap)} paths')

        for path, flags in self.canonmap.values():
            logger.info(f'> Listening on path {path} for flags {iflags.from_mask(flags)}')

            for (callback, pattern, debounce, delay, *_) in self.router.routemap[path]:
                logger.info(f'| > {pattern} -> {callback.__name__} (debounce {debounce}ms, delay {delay}ms)')

        while True:
            rlist, _, _ = select.select(
                [self.inotify.fileno(), self.read_fd], [], []
            )

            if self.inotify.fileno() in rlist:
                events = self.inotify.read(timeout=0)
                self.handle_events(events)

            # check for written stop byte 
            if self.read_fd in rlist:
                os.close(self.read_fd)
                self.inotify.close()
                return

    def update_moved_from(self, path, lead):
        '''
        Update directories on `MOVED_FROM` events. This method gets the existing WD,
        removes the old path associated with that WD from the `watchmap` (preventing
        events originating from this old path when the new path, which has the _same WD_,
        receives an inotify event), and queues the (WD, base-path) tuple to be matched
        later in a `MOVED_TO` handler.

        This method isn't a part of a `MOVED_TO` handler because it may be called without
        ever having a `MOVED_TO` that follows up. We respond right away in `handle_events`
        to `MOVED_FROM` events, keeping the `watchmap` in sync, regardless of whether we
        can expect a `MOVED_TO` to sweep through after the fact.

        Note that the `lead` is unique for a given WD and base path. WDs are unique for
        filepaths, but inotify uses the same WD for new directories when they experience a
        rename (it's the same inode). However, during such a transition, the `watchmap`
        can see two different entries for the same WD and basepath: the old tracked path,
        and the newly named one (again, still the same inode). So: this method can be
        called 1) directly from `MOVED_FROM` events, preemptively wiping the old path from
        the tracked dicts, or 2) during handling of a `MOVED_TO` event (in case we don't
        allow `MOVED_FROM` events, for instance), given both the new and old paths can be
        seen in the `watchmap`.
        '''
        wd = self.pathmap.get(Path(path, lead))
        logger.debug(f'> MOVED_FROM update, [{Path(path, lead)}] in pathmap as [{wd}]')
        if wd is None: return

        if self.watchmap[wd].pop((path, lead), None):
            logger.debug(f'> MOVED_FROM update, popped from watchmap')
            self.unmatched_move_froms[wd][path] = lead

    def update_moved_to(self, path, lead):
        '''
        Construct synthetic MOVED events. Events are constructed from the path's WD. If
        the provided path is not watched, an empty list of events is returned.

        Note: Design details
            This method is nuanced. It can only be called once a `MOVED_TO` occurs, since
            we can't act on a `MOVED_FROM` (we don't know the new target location to look
            so we can send file events). When called, we first look for the path's WD in
            the `pathmap`. We then check if this WD points to more than one entry with the
            same base path (WDs are unique to the path; under the same WD, the same base
            path implies the same lead). If so, we know one is the outdated path, and we
            push the outdated lead to `update_moved_from`. This would be evidence that the
            `MOVED_FROM` event for the move operation wasn't handled in the main event
            handling loop. We then check for unmatched move-froms, which should provide
            any renamed directories, regardless of whether `MOVED_FROM`s were allowed, to
            be detected. Finally, the appropriate `MOVED_FROM`s and `MOVED_TO`s are
            handled. To ensure only the correct events match upon handling, we do the
            following:

            - First, if a `MOVED_FROM` path is not available, we assume it wasn't queued
              by the event and not a watched flag. Given we by default ensure MOVED events
              are tracked, regardless of listened paths, this shouldn't be possible, but
              if this standard were to change, we won't recursively respond to
              `MOVED_FROM`s. This will mean that we can't prevent events from being
              matched to old directory names (we've rooted out the ability to tell when
              they've changed), and thus can't remove them from the `watchpath`
              accordingly. (One functional caveat here: this MOVED_TO handling method
              explicitly calls `updated_moved_from`, which should clean up lingering
              renamed path targets. This happens recursively if we're watching MOVED_TOs,
              so even if standards do change and you don't watch `MOVED_FROM`s, you'll
              still get clean up for free due to the robustness of this method.
            - If a `MOVED_FROM` lead is found, either due to an inferred matching base
              lingering in the `watchmap` or through previously handled `MOVED_FROM`
              response, add this path/lead back to the `watchmap`, remove the new
              path/lead, and call `handle_events` for the synthetic `MOVED_FROM` events
              across files and directories.  Once finished, again remove the old path/lead
              and add back the new one.
            - Submit `MOVED_TO` events to `handle_events`. This will recursively propagate for
              subdirectories, each submitting their own `update_moved_to` call, resetting
              its own outdated leads and changing them back, all the way down to the
              bottom. 

            In the odd case where `MOVED_FROM` is registered but not `MOVED_TO`, you will
            simply remove the directory causing a `MOVED_FROM` event, with no recursive
            propagation. This should likely be changed.
        '''
        fullpath = Path(path, lead)

        wd = self.pathmap.get(fullpath)
        if wd is None:
            logger.debug(f'Directory [{fullpath}] moved, but is not watched, ignoring')
            return []

        # inspect registered paths with same WD -- looking for same base path but diff lead
        # will be empty if explicitly handled by a MOVED_FROM -- else inferred from watchmap
        matching_bases = [pl for pl in self.watchmap[wd] if pl[0] == path and pl[1] != lead]

        # there should be at most one of these, but handle iteratively
        for matching_base, old_lead in matching_bases:
            self.update_moved_from(matching_base, old_lead)

        # explicit queries for files & dirs faster (tested) than filtering a single query
        # using `Path.is_dir`; handle files, then subdirectories
        moved_from_events = []
        moved_to_events = []
        for file in util.path.iter_glob_paths('*', fullpath, no_dir=True):
            moved_from_events.append(iEvent(wd=wd, mask=iflags.MOVED_FROM, cookie=0, name=file.name))
            moved_to_events.append(iEvent(wd=wd, mask=iflags.MOVED_TO, cookie=0, name=file.name))

        for subdir in util.path.iter_glob_paths('*/', fullpath):
            moved_from_mask = iflags.MOVED_FROM | iflags.ISDIR
            moved_from_events.append(iEvent(wd=wd, mask=moved_from_mask, cookie=0, name=subdir.name))

            moved_to_mask = iflags.MOVED_TO | iflags.ISDIR
            moved_to_events.append(iEvent(wd=wd, mask=moved_to_mask, cookie=0, name=subdir.name))

        # check for unmatched moved froms -- should be enqueued in event loop or just above
        moved_from_lead = self.unmatched_move_froms.get(wd, {}).pop(path, None)
        if moved_from_lead is None:
            logger.debug(f'Couldn\'t find MOVED_FROM origin, just yielding MOVED_TO events')
        else:
            # temporarily remove new path, add old path to allow MOVED_FROMs to seep through
            flags = self.watchmap[wd].pop((path, lead)) # remove new
            self.watchmap[wd][(path, moved_from_lead)] = flags # add old
            self.handle_events(moved_from_events)

            self.watchmap[wd].pop((path, moved_from_lead)) # remove old
            self.watchmap[wd][(path, lead)] = flags # add back new

        self.handle_events(moved_to_events)

    def handle_events(self, events):
        '''
        Note:
            If `handle_events` is called externally, note that this loop will block in the
            calling thread until the jobs have been submitted. It will _not_ block until
            jobs have completed, however, as a list of futures is returned. The calling
            Watcher instance may have already been started, in which case `run()` will
            already be executing in a separate thread. Calling this method externally will
            not interfere with this loop insofar as it adds jobs to the same thread pool.

            Because this method only submits jobs associated with the provided `events`,
            the calling thread can await the returned list of futures and be confident
            that top-level callbacks associated with these file events have completed. Do
            note that, if the Watcher has already been started, any propagating file
            events will be picked up and possibly processed simultaneously (although their
            associated callbacks will have nothing to do with the returned list of futures).
        '''
        from execlog.router import Event
        
        for event in events:
            # hard coded ignores
            if util.path.glob_match(event.name, IGNORE_PATTERNS): continue

            mask_flags = iflags.from_mask(event.mask)

            if event.wd not in self.watchmap:
                raise ValueError(f'Watcher fired for untracked descriptor origin: {event}')

            moved_froms = []
            moved_tos   = []
            for (path, lead), flags in self.watchmap[event.wd].items():
                relpath = Path(lead, event.name)
                abspath = Path(path, relpath)

                # add new directories 
                if iflags.ISDIR in mask_flags:
                    if iflags.CREATE in mask_flags:
                        logger.debug(f'New directory detected [{relpath}]')
                        self._add_watch_recursive(path, flags, lead=relpath)

                    if iflags.MOVED_FROM in mask_flags:
                        moved_froms.append((path, relpath))

                    if iflags.MOVED_TO in mask_flags:
                        moved_tos.append((path, relpath))

                    continue

                logger.debug(f'Watcher fired for [{relpath}]: {mask_flags}')
                
                route_event = Event(endpoint=str(path), name=str(relpath), action=mask_flags)
                self.router.submit(route_event)

            # handle renamed directories; old dir was being watched if these flags
            # match. The same WD is used by iNotify for the new dir, so
            # recursively update explicitly stored paths.
            for path, lead in moved_froms:
                logger.debug(f'Directory moved, removing old [{lead}]')
                self.update_moved_from(path, lead)

            for path, lead in moved_tos:
                logger.debug(f'Directory moved, adding new [{lead}]')
                self._add_watch(path, flags, lead=lead)
                self.update_moved_to(path, lead)

    def stop(self):
        logger.info("Stopping listener...")

        if self.router.thread_pool is not None:
            self.router.thread_pool.shutdown()
        
        # request INotify stop by writing in the pipe, checked in watch loop
        if not self.write.closed:
            self.write.write(b"\x00")
            self.write.close()
