import logging
from pathlib import Path
from concurrent.futures import as_completed

from tqdm import tqdm
from colorama import Fore, Back, Style
from inotify_simple import flags as iflags

from co3.resources import DiskResource
from co3 import Differ, Syncer, Database

from execlog.event import Event
from execlog.routers import PathRouter
from execlog.util.generic import color_text


logger = logging.getLogger(__name__)

class PathDiffer(Differ[Path]):
    def __init__(
        self,
        database: Database,
    ):
        super().__init__(DiskResource(), database)

    def l_transform(self, item):
        '''
        Transform ``(path, head)`` tuple from ``DiskResource``.
        '''
        return Path(*item)

class PathRouterSyncer(Syncer[Path]):
    def __init__(
        self,
        differ: PathDiffer,
        router: PathRouter,
    ):
        super().__init__(differ)
        self.router = router

    def _construct_event(
        self,
        fpath: str | Path,
        endpoint: str | Path,
        action: bytes
    ):
        return Event(
            endpoint=str(endpoint),
            name=str(Path(fpath).relative_to(endpoint)),
            action=[action], # synthetic action to match any flag filters
        )

    def handle_l_excl(self, path: Path, disk_pairs: list):
        '''
        Handle disk exclusive paths (i.e., those added to disk since last sync).
        '''
        return [
            self._construct_event(str(path), endpoint, iflags.CREATE)
            for endpoint, _ in disk_pairs
        ]

    def handle_r_excl(self, path: Path, db_vals: list):
        '''
        Handle database exclusive paths (i.e., those deleted from disk since last sync).
        Searches for matching endpoints under the attached router and creates
        corresponding events.

        .. admonition:: On lack of endpoints

            This method handles database exclusive items, i.e., paths no longer on disk
            but still in the database. For typical Router designs, it is not important to
            preserve possible endpoints of origin for this kind of event; what matters is
            the absolute path of the file to be removed. In general, file events are
            associated solely with a path, but we in some cases may be sensitive to the
            base path seen to be "triggering" that file event, as router methods can hook
            in to specific endpoints. This has somewhat dubious effects, as multiple
            events (with the same action) are dispatched for the same file, purely to
            observe the Router convention of endpoints and allowing independent
            trajectories through the execution sequence.

            One concern here is that you might, in theory, want to respond to the same
            file deletion event in different ways under different endpoints. This will be
            accessible when picking up such an event live, as endpoints are grouped by
            watch descriptor and can be all be triggered from the single file event. This
            is the one case where we can't *really* simulate the event taking place with
            the available data, and instead have to peer into the router to see what root
            paths the file could theoretically trigger. Most of the time, this won't be
            too problematic, since we'll be watching the same paths and can tell where a
            deleted file would've been. But there are cases where a watch path endpoint
            may be abandoned, and thus no callback will be there to receive the DELETE
            event. *Routers should heavily consider implementing a global DELETE handler
            to prevent these cases if it's critical to respond to deletions.* Otherwise,
            we still make an attempt to propagate under appropriate endpoints, allowing
            for possible "deconstructor-like" behavior of specific filetypes (e.g.,
            cleaning up auxiliary elements, writing to a log, creating a backup, etc).
        '''
        return [
            self._construct_event(str(path), str(endpoint), iflags.DELETE)
            for endpoint in self.router.routemap
            if Path(path).is_relative_to(Path(endpoint))
        ]

    def handle_lr_int(self, path: Path, path_tuples: tuple[list, list]):
        '''
        Handle paths reflected both in the database and on disk.

        Paths only reach this method if still present after being passed through
        ``filter_diff_sets``, which will filter out those files that are up-to-date in the
        database.
        '''
        return [
            self._construct_event(str(path), endpoint, iflags.MODIFY)
            for endpoint, _ in path_tuples[0]
        ]

    def filter_diff_sets(self, l_excl, r_excl, lr_int):
        total_disk_files = len(l_excl) + len(lr_int)
        total_joint_files = len(lr_int)

        def file_out_of_sync(p): 
            _, db_el = lr_int[p]
            db_mtime = float(db_el[0].get('mtime','0'))
            disk_mtime = Path(p).stat().st_mtime

            return disk_mtime > db_mtime

        lr_int = {p:v for p,v in lr_int.items() if file_out_of_sync(p)}

        # compute out-of-sync details
        oos_count = len(l_excl) + len(lr_int)
        oos_prcnt = oos_count / max(total_disk_files, 1) * 100

        logger.info(color_text(f'{len(l_excl)} new files to add',                                Fore.GREEN)),
        logger.info(color_text(f'{len(lr_int)} modified files [{total_joint_files} up-to-date]', Fore.YELLOW)),
        logger.info(color_text(f'{len(r_excl)} files to remove',                                 Fore.RED)),
        logger.info(color_text(f'({oos_prcnt:.2f}%) of disk files out-of-sync',                  Style.DIM)),

        return l_excl, r_excl, lr_int

    def process_chunk(self, event_sets):
        chunk_events = [e for event_set in event_sets for e in event_set]

        # 1) flush synthetic events for the batch through the chained router
        # 2) block until completed and sweep up the collected inserts
        event_futures = self.router.submit(chunk_events)

        # note: we structure this future waiting like this for the TQDM view
        results = []
        for future in tqdm(
            as_completed(event_futures),
            total=len(event_futures),
            desc=f'Awaiting chunk futures [submitted {len(event_futures)}]'
        ):
            try:
                if not future.cancelled():
                    results.append(future.result())
            except Exception as e:
                logger.warning(f"Sync job failed with exception {e}")

        return results

    def shutdown(self):
        super().shutdown()
        self.router.shutdown()

