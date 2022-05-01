//! There are a few components the storage machinery consists of:
//!
//! * [`RemoteStorage`] that is used to interact with an arbitrary external storage
//!
//! * synchronization logic at [`storage_sync`] module that keeps pageserver state (both runtime one and the workdir files) and storage state in sync.
//! Synchronization internals are split into submodules
//!     * [`storage_sync::index`] to keep track of remote tenant files, the metadata and their mappings to local files
//!     * [`storage_sync::upload`] and [`storage_sync::download`] to manage archive creation and upload; download and extraction, respectively
//!
//! * public API via to interact with the external world:
//!     * [`start_local_timeline_sync`] to launch a background async loop to handle the synchronization
//!     * [`schedule_timeline_checkpoint_upload`] and [`schedule_timeline_download`] to enqueue a new upload and download tasks,
//!       to be processed by the async loop
//!
//! Here's a schematic overview of all interactions backup and the rest of the pageserver perform:
//!
//! +------------------------+                                    +--------->-------+
//! |                        |  - - - (init async loop) - - - ->  |                 |
//! |                        |                                    |                 |
//! |                        |  ------------------------------->  |      async      |
//! |       pageserver       |    (enqueue timeline sync task)    | upload/download |
//! |                        |                                    |      loop       |
//! |                        |  <-------------------------------  |                 |
//! |                        |  (apply new timeline sync states)  |                 |
//! +------------------------+                                    +---------<-------+
//!                                                                         |
//!                                                                         |
//!                                          CRUD layer file operations     |
//!                                     (upload/download/delete/list, etc.) |
//!                                                                         V
//!                                                            +------------------------+
//!                                                            |                        |
//!                                                            | [`RemoteStorage`] impl |
//!                                                            |                        |
//!                                                            | pageserver assumes it  |
//!                                                            | owns exclusive write   |
//!                                                            | access to this storage |
//!                                                            +------------------------+
//!
//! First, during startup, the pageserver inits the storage sync thread with the async loop, or leaves the loop uninitialised, if configured so.
//! The loop inits the storage connection and checks the remote files stored.
//! This is done once at startup only, relying on the fact that pageserver uses the storage alone (ergo, nobody else uploads the files to the storage but this server).
//! Based on the remote storage data, the sync logic immediately schedules sync tasks for local timelines and reports about remote only timelines to pageserver, so it can
//! query their downloads later if they are accessed.
//!
//! Some time later, during pageserver checkpoints, in-memory data is flushed onto disk along with its metadata.
//! If the storage sync loop was successfully started before, pageserver schedules the new checkpoint file uploads after every checkpoint.
//! The checkpoint uploads are disabled, if no remote storage configuration is provided (no sync loop is started this way either).
//! See [`crate::layered_repository`] for the upload calls and the adjacent logic.
//!
//! Synchronization logic is able to communicate back with updated timeline sync states, [`crate::repository::TimelineSyncStatusUpdate`],
//! submitted via [`crate::tenant_mgr::apply_timeline_sync_status_updates`] function. Tenant manager applies corresponding timeline updates in pageserver's in-memory state.
//! Such submissions happen in two cases:
//! * once after the sync loop startup, to signal pageserver which timelines will be synchronized in the near future
//! * after every loop step, in case a timeline needs to be reloaded or evicted from pageserver's memory
//!
//! When the pageserver terminates, the sync loop finishes a current sync task (if any) and exits.
//!
//! The storage logic considers `image` as a set of local files (layers), fully representing a certain timeline at given moment (identified with `disk_consistent_lsn` from the corresponding `metadata` file).
//! Timeline can change its state, by adding more files on disk and advancing its `disk_consistent_lsn`: this happens after pageserver checkpointing and is followed
//! by the storage upload, if enabled.
//! Yet timeline cannot alter already existing files, and cannot remove those too: only a GC process is capable of removing unused files.
//! This way, remote storage synchronization relies on the fact that every checkpoint is incremental and local files are "immutable":
//! * when a certain checkpoint gets uploaded, the sync loop remembers the fact, preventing further reuploads of the same state
//! * no files are deleted from either local or remote storage, only the missing ones locally/remotely get downloaded/uploaded, local metadata file will be overwritten
//! when the newer image is downloaded
//!
//! Pageserver maintains similar to the local file structure remotely: all layer files are uploaded with the same names under the same directory structure.
//! Yet instead of keeping the `metadata` file remotely, we wrap it with more data in [`IndexShard`], containing the list of remote files.
//! This file gets read to populate the cache, if the remote timeline data is missing from it and gets updated after every successful download.
//! This way, we optimize S3 storage access by not running the `S3 list` command that could be expencive and slow: knowing both [`ZTenantId`] and [`ZTimelineId`],
//! we can always reconstruct the path to the timeline, use this to get the same path on the remote storage and retrive its shard contents, if needed, same as any layer files.
//!
//! By default, pageserver reads the remote storage index data only for timelines located locally, to synchronize those, if needed.
//! Bulk index data download happens only initially, on pageserer startup. The rest of the remote storage stays unknown to pageserver and loaded on demand only,
//! when a new timeline is scheduled for the download.
//!
//! NOTES:
//! * pageserver assumes it has exclusive write access to the remote storage. If supported, the way multiple pageservers can be separated in the same storage
//! (i.e. using different directories in the local filesystem external storage), but totally up to the storage implementation and not covered with the trait API.
//!
//! * the sync tasks may not processed immediately after the submission: if they error and get re-enqueued, their execution might be backed off to ensure error cap is not exceeded too fast.
//! The sync queue processing also happens in batches, so the sync tasks can wait in the queue for some time.
//!
//! A synchronization logic for the [`RemoteStorage`] and pageserver in-memory state to ensure correct synchronizations
//! between local tenant files and their counterparts from the remote storage.
//!
//! The synchronization does not aim to be immediate, yet eventually consistent.
//! Synchronization is done with the queue being emptied via separate thread asynchronously,
//! attempting to fully store pageserver's local data on the remote storage in a custom format, beneficial for storing.
//!
//! A queue is implemented in the [`sync_queue`] module as a pair of sender and receiver channels, to block on zero tasks instead of checking the queue.
//! The pair's shared buffer of a fixed size serves as an implicit queue, holding [`SyncTask`] for local files upload/download operations.
//!
//! The queue gets emptied by a single thread with the loop, that polls the tasks in batches of deduplicated tasks.
//! A task from the batch corresponds to a single timeline, with its files to sync merged together: given that only one task sync loop step is active at a time,
//! timeline uploads and downloads can happen concurrently, in no particular order due to incremental nature of the timeline layers.
//! Deletion happens only after a successful upload only, otherwise the compation output might make the timeline inconsistent until both tasks are fully processed without errors.
//! Upload and download update the remote data (inmemory index and S3 json index part file) only after every layer is successfully synchronized, while the deletion task
//! does otherwise: it requires to have the remote data updated first succesfully: blob files will be invisible to pageserver this way.
//!
//! During the loop startup, an initial [`RemoteTimelineIndex`] state is constructed via downloading and merging the index data for all timelines,
//! present locally.
//! It's enough to poll such timelines' remote state once on startup only, due to an agreement that only one pageserver at a time has an exclusive
//! write access to remote portion of timelines that are attached to the pagegserver.
//! The index state is used to issue initial sync tasks, if needed:
//! * all timelines with local state behind the remote gets download tasks scheduled.
//! Such timelines are considered "remote" before the download succeeds, so a number of operations (gc, checkpoints) on that timeline are unavailable
//! before up-to-date layers and metadata file are downloaded locally.
//! * all newer local state gets scheduled for upload, such timelines are "local" and fully operational
//! * remote timelines not present locally are unknown to pageserver, but can be downloaded on a separate request
//!
//! Then, the index is shared across pageserver under [`RemoteIndex`] guard to ensure proper synchronization.
//! The remote index gets updated after very remote storage change (after an upload), same as the index part files remotely.
//!
//! Remote timeline contains a set of layer files, created during checkpoint(s) and the serialized [`IndexPart`] file with timeline metadata and all remote layer paths inside.
//! Those paths are used instead of `S3 list` command to avoid its slowliness and expenciveness for big amount of files.
//! If the index part does not contain some file path but it's present remotely, such file is invisible to pageserver and ignored.
//! Among other tasks, the index is used to prevent invalid uploads and non-existing downloads on demand, refer to [`index`] for more details.
//!
//! Index construction is currently the only place where the storage sync can return an [`Err`] to the user.
//! New sync tasks are accepted via [`schedule_layer_upload`], [`schedule_layer_download`] and [`schedule_layer_delete`] functions,
//! disregarding of the corresponding loop startup.
//! It's up to the caller to avoid synchronizations if the loop is disabled: otherwise, the sync tasks will be ignored.
//! After the initial state is loaded into memory and the loop starts, any further [`Err`] results do not stop the loop, but rather
//! reschedule the same task, with possibly less files to sync:
//! * download tasks currently never replace existing local file with metadata file as an exception
//! (but this is a subject to change when checksum checks are implemented: all files could get overwritten on a checksum mismatch)
//! * download tasks carry the information of skipped acrhives, so resubmissions are not downloading successfully processed layers again
//! * downloads do not contain any actual files to download, so that "external", sync pageserver code is able to schedule the timeline download
//! without accessing any extra information about its files.
//!
//! Uploads and downloads sync layer files in arbitrary order, but only after all layer files are synched the local metadada (for download) and remote index part (for upload) are updated,
//! to avoid having a corrupt state without the relevant layer files.
//! Refer to [`upload`] and [`download`] for more details.
//!
//! Synchronization never removes any local files from pageserver workdir or remote files from the remote storage, yet there could be overwrites of the same files (index part and metadata file updates, future checksum mismatch fixes).
//! NOTE: No real contents or checksum check happens right now and is a subject to improve later.
//!
//! After the whole timeline is downloaded, [`crate::tenant_mgr::apply_timeline_sync_status_updates`] function is used to update pageserver memory stage for the timeline processed.
//!
//! When pageserver signals shutdown, current sync task gets finished and the loop exists.

mod delete;
mod download;
pub mod index;
mod upload;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    ffi::OsStr,
    fmt::Debug,
    num::{NonZeroU32, NonZeroUsize},
    ops::ControlFlow,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{bail, Context};
use futures::stream::{FuturesUnordered, StreamExt};
use lazy_static::lazy_static;
use remote_storage::{GenericRemoteStorage, RemoteStorage};
use tokio::{
    fs,
    runtime::Runtime,
    sync::mpsc::{self, UnboundedReceiver},
    time::{Duration, Instant},
};
use tracing::*;

use self::{
    delete::delete_timeline_layers,
    download::{download_timeline_layers, DownloadedTimeline},
    index::{IndexPart, RemoteTimeline, RemoteTimelineIndex},
    upload::{upload_index_part, upload_timeline_layers, UploadedTimeline},
};
use crate::{
    config::PageServerConf,
    layered_repository::{
        ephemeral_file::is_ephemeral_file,
        metadata::{metadata_path, TimelineMetadata, METADATA_FILE_NAME},
        LayeredRepository,
    },
    repository::TimelineSyncStatusUpdate,
    storage_sync::{self, index::RemoteIndex},
    tenant_mgr::apply_timeline_sync_status_updates,
    thread_mgr,
    thread_mgr::ThreadKind,
};

use metrics::{
    register_histogram_vec, register_int_counter, register_int_gauge, HistogramVec, IntCounter,
    IntGauge,
};
use utils::zid::{ZTenantId, ZTenantTimelineId, ZTimelineId};

pub use self::download::download_index_part;
pub use self::download::TEMP_DOWNLOAD_EXTENSION;

lazy_static! {
    static ref REMAINING_SYNC_ITEMS: IntGauge = register_int_gauge!(
        "pageserver_remote_storage_remaining_sync_items",
        "Number of storage sync items left in the queue"
    )
    .expect("failed to register pageserver remote storage remaining sync items int gauge");
    static ref FATAL_TASK_FAILURES: IntCounter = register_int_counter!(
        "pageserver_remote_storage_fatal_task_failures",
        "Number of critically failed tasks"
    )
    .expect("failed to register pageserver remote storage remaining sync items int gauge");
    static ref IMAGE_SYNC_TIME: HistogramVec = register_histogram_vec!(
        "pageserver_remote_storage_image_sync_time",
        "Time took to synchronize (download or upload) a whole pageserver image. \
        Grouped by `operation_kind` (upload|download) and `status` (success|failure)",
        &["operation_kind", "status"],
        vec![
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0, 7.0,
            8.0, 9.0, 10.0, 12.5, 15.0, 17.5, 20.0
        ]
    )
    .expect("failed to register pageserver image sync time histogram vec");
}

/// A timeline status to share with pageserver's sync counterpart,
/// after comparing local and remote timeline state.
#[derive(Clone, Copy, Debug)]
pub enum LocalTimelineInitStatus {
    /// The timeline has every remote layer present locally.
    /// There could be some layers requiring uploading,
    /// but this does not block the timeline from any user interaction.
    LocallyComplete,
    /// A timeline has some files remotely, that are not present locally and need downloading.
    /// Downloading might update timeline's metadata locally and current pageserver logic deals with local layers only,
    /// so the data needs to be downloaded first before the timeline can be used.
    NeedsSync,
}

type LocalTimelineInitStatuses = HashMap<ZTenantId, HashMap<ZTimelineId, LocalTimelineInitStatus>>;

/// A structure to combine all synchronization data to share with pageserver after a successful sync loop initialization.
/// Successful initialization includes a case when sync loop is not started, in which case the startup data is returned still,
/// to simplify the received code.
pub struct SyncStartupData {
    pub remote_index: RemoteIndex,
    pub local_timeline_init_statuses: LocalTimelineInitStatuses,
}

/// Based on the config, initiates the remote storage connection and starts a separate thread
/// that ensures that pageserver and the remote storage are in sync with each other.
/// If no external configuration connection given, no thread or storage initialization is done.
/// Along with that, scans tenant files local and remote (if the sync gets enabled) to check the initial timeline states.
pub fn start_local_timeline_sync(
    config: &'static PageServerConf,
) -> anyhow::Result<SyncStartupData> {
    let local_timeline_files = local_tenant_timeline_files(config)
        .context("Failed to collect local tenant timeline files")?;

    match config.remote_storage_config.as_ref() {
        Some(storage_config) => {
            match GenericRemoteStorage::new(config.workdir.clone(), storage_config)
                .context("Failed to init the generic remote storage")?
            {
                GenericRemoteStorage::Local(local_fs_storage) => {
                    storage_sync::spawn_storage_sync_thread(
                        config,
                        local_timeline_files,
                        local_fs_storage,
                        storage_config.max_concurrent_syncs,
                        storage_config.max_sync_errors,
                    )
                }
                GenericRemoteStorage::S3(s3_bucket_storage) => {
                    storage_sync::spawn_storage_sync_thread(
                        config,
                        local_timeline_files,
                        s3_bucket_storage,
                        storage_config.max_concurrent_syncs,
                        storage_config.max_sync_errors,
                    )
                }
            }
            .context("Failed to spawn the storage sync thread")
        }
        None => {
            info!("No remote storage configured, skipping storage sync, considering all local timelines with correct metadata files enabled");
            let mut local_timeline_init_statuses = LocalTimelineInitStatuses::new();
            for (
                ZTenantTimelineId {
                    tenant_id,
                    timeline_id,
                },
                _,
            ) in local_timeline_files
            {
                local_timeline_init_statuses
                    .entry(tenant_id)
                    .or_default()
                    .insert(timeline_id, LocalTimelineInitStatus::LocallyComplete);
            }
            Ok(SyncStartupData {
                local_timeline_init_statuses,
                remote_index: RemoteIndex::empty(),
            })
        }
    }
}

fn local_tenant_timeline_files(
    config: &'static PageServerConf,
) -> anyhow::Result<HashMap<ZTenantTimelineId, (TimelineMetadata, HashSet<PathBuf>)>> {
    let mut local_tenant_timeline_files = HashMap::new();
    let tenants_dir = config.tenants_path();
    for tenants_dir_entry in std::fs::read_dir(&tenants_dir)
        .with_context(|| format!("Failed to list tenants dir {}", tenants_dir.display()))?
    {
        match &tenants_dir_entry {
            Ok(tenants_dir_entry) => {
                match collect_timelines_for_tenant(config, &tenants_dir_entry.path()) {
                    Ok(collected_files) => {
                        local_tenant_timeline_files.extend(collected_files.into_iter())
                    }
                    Err(e) => error!(
                        "Failed to collect tenant files from dir '{}' for entry {:?}, reason: {:#}",
                        tenants_dir.display(),
                        tenants_dir_entry,
                        e
                    ),
                }
            }
            Err(e) => error!(
                "Failed to list tenants dir entry {:?} in directory {}, reason: {:?}",
                tenants_dir_entry,
                tenants_dir.display(),
                e
            ),
        }
    }

    Ok(local_tenant_timeline_files)
}

fn collect_timelines_for_tenant(
    config: &'static PageServerConf,
    tenant_path: &Path,
) -> anyhow::Result<HashMap<ZTenantTimelineId, (TimelineMetadata, HashSet<PathBuf>)>> {
    let mut timelines = HashMap::new();
    let tenant_id = tenant_path
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or_default()
        .parse::<ZTenantId>()
        .context("Could not parse tenant id out of the tenant dir name")?;
    let timelines_dir = config.timelines_path(&tenant_id);

    for timelines_dir_entry in std::fs::read_dir(&timelines_dir).with_context(|| {
        format!(
            "Failed to list timelines dir entry for tenant {}",
            tenant_id
        )
    })? {
        match timelines_dir_entry {
            Ok(timelines_dir_entry) => {
                let timeline_path = timelines_dir_entry.path();
                match collect_timeline_files(&timeline_path) {
                    Ok((timeline_id, metadata, timeline_files)) => {
                        timelines.insert(
                            ZTenantTimelineId {
                                tenant_id,
                                timeline_id,
                            },
                            (metadata, timeline_files),
                        );
                    }
                    Err(e) => error!(
                        "Failed to process timeline dir contents at '{}', reason: {:?}",
                        timeline_path.display(),
                        e
                    ),
                }
            }
            Err(e) => error!(
                "Failed to list timelines for entry tenant {}, reason: {:?}",
                tenant_id, e
            ),
        }
    }

    Ok(timelines)
}

// discover timeline files and extract timeline metadata
//  NOTE: ephemeral files are excluded from the list
fn collect_timeline_files(
    timeline_dir: &Path,
) -> anyhow::Result<(ZTimelineId, TimelineMetadata, HashSet<PathBuf>)> {
    let mut timeline_files = HashSet::new();
    let mut timeline_metadata_path = None;

    let timeline_id = timeline_dir
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or_default()
        .parse::<ZTimelineId>()
        .context("Could not parse timeline id out of the timeline dir name")?;
    let timeline_dir_entries =
        std::fs::read_dir(&timeline_dir).context("Failed to list timeline dir contents")?;
    for entry in timeline_dir_entries {
        let entry_path = entry.context("Failed to list timeline dir entry")?.path();
        if entry_path.is_file() {
            if entry_path.file_name().and_then(OsStr::to_str) == Some(METADATA_FILE_NAME) {
                timeline_metadata_path = Some(entry_path);
            } else if is_ephemeral_file(&entry_path.file_name().unwrap().to_string_lossy()) {
                debug!("skipping ephemeral file {}", entry_path.display());
                continue;
            } else if entry_path.extension().and_then(OsStr::to_str)
                == Some(TEMP_DOWNLOAD_EXTENSION)
            {
                info!("removing temp download file at {}", entry_path.display());
                std::fs::remove_file(&entry_path).with_context(|| {
                    format!(
                        "failed to remove temp download file at {}",
                        entry_path.display()
                    )
                })?;
            } else {
                timeline_files.insert(entry_path);
            }
        }
    }

    // FIXME (rodionov) if attach call succeeded, and then pageserver is restarted before download is completed
    //   then attach is lost. There would be no retries for that,
    //   initial collect will fail because there is no metadata.
    //   We either need to start download if we see empty dir after restart or attach caller should
    //   be aware of that and retry attach if awaits_download for timeline switched from true to false
    //   but timelinne didnt appear locally.
    //   Check what happens with remote index in that case.
    let timeline_metadata_path = match timeline_metadata_path {
        Some(path) => path,
        None => bail!("No metadata file found in the timeline directory"),
    };
    let metadata = TimelineMetadata::from_bytes(
        &std::fs::read(&timeline_metadata_path).context("Failed to read timeline metadata file")?,
    )
    .context("Failed to parse timeline metadata file bytes")?;

    Ok((timeline_id, metadata, timeline_files))
}

/// Wraps mpsc channel bits around into a queue interface.
/// mpsc approach was picked to allow blocking the sync loop if no tasks are present, to avoid meaningless spinning.
mod sync_queue {
    use std::{
        collections::{HashMap, HashSet},
        num::NonZeroUsize,
        ops::ControlFlow,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use anyhow::anyhow;
    use once_cell::sync::OnceCell;
    use tokio::sync::mpsc::{error::TryRecvError, UnboundedReceiver, UnboundedSender};
    use tracing::{debug, warn};

    use super::{SyncTask, SyncTaskBatch};
    use utils::zid::ZTenantTimelineId;

    static SENDER: OnceCell<UnboundedSender<(ZTenantTimelineId, SyncTask)>> = OnceCell::new();
    static LENGTH: AtomicUsize = AtomicUsize::new(0);

    /// Initializes the queue with the given sender channel that is used to put the tasks into later.
    /// Errors if called more than once.
    pub fn init(sender: UnboundedSender<(ZTenantTimelineId, SyncTask)>) -> anyhow::Result<()> {
        SENDER
            .set(sender)
            .map_err(|_sender| anyhow!("sync queue was already initialized"))?;
        Ok(())
    }

    /// Adds a new task to the queue, if the queue was initialized, returning `true` on success.
    /// On any error, or if the queue was not initialized, the task gets dropped (not scheduled) and `false` is returned.
    pub fn push(sync_id: ZTenantTimelineId, new_task: SyncTask) -> bool {
        if let Some(sender) = SENDER.get() {
            match sender.send((sync_id, new_task)) {
                Err(e) => {
                    warn!("Failed to enqueue a sync task: the receiver is dropped: {e}");
                    false
                }
                Ok(()) => {
                    LENGTH.fetch_add(1, Ordering::Relaxed);
                    true
                }
            }
        } else {
            warn!("Failed to enqueue a sync task: the sender is not initialized");
            false
        }
    }

    /// Polls a new task from the queue, using its receiver counterpart.
    /// Does not block if the queue is empty, returning [`None`] instead.
    /// Needed to correctly track the queue length.
    async fn next_task(
        receiver: &mut UnboundedReceiver<(ZTenantTimelineId, SyncTask)>,
    ) -> Option<(ZTenantTimelineId, SyncTask)> {
        let task = receiver.recv().await;
        if task.is_some() {
            LENGTH.fetch_sub(1, Ordering::Relaxed);
        }
        task
    }

    /// Fetches a task batch, not bigger than the given limit.
    /// Not blocking, can return fewer tasks if the queue does not contain enough.
    /// Batch tasks are split by timelines, with all related tasks merged into one (download/upload)
    /// or two (download and upload, if both were found in the queue during batch construction).
    pub(super) async fn next_task_batch(
        receiver: &mut UnboundedReceiver<(ZTenantTimelineId, SyncTask)>,
        max_timelines_to_sync: NonZeroUsize,
    ) -> ControlFlow<(), HashMap<ZTenantTimelineId, SyncTaskBatch>> {
        // request the first task in blocking fashion to do less meaningless work
        let (first_sync_id, first_task) = if let Some(first_task) = next_task(receiver).await {
            first_task
        } else {
            debug!("Queue sender part was dropped, aborting");
            return ControlFlow::Break(());
        };

        let max_timelines_to_sync = max_timelines_to_sync.get();
        let mut batched_timelines = HashSet::with_capacity(max_timelines_to_sync);
        batched_timelines.insert(first_sync_id.timeline_id);

        let mut tasks = HashMap::new();
        tasks.insert(first_sync_id, SyncTaskBatch::new(first_task));

        loop {
            if batched_timelines.len() >= max_timelines_to_sync {
                debug!(
                    "Filled a full task batch with {} timeline sync operations",
                    batched_timelines.len()
                );
                break;
            }

            match receiver.try_recv() {
                Ok((sync_id, new_task)) => {
                    LENGTH.fetch_sub(1, Ordering::Relaxed);
                    tasks.entry(sync_id).or_default().add(new_task);
                    batched_timelines.insert(sync_id.timeline_id);
                }
                Err(TryRecvError::Disconnected) => {
                    debug!("Sender disconnected, batch collection aborted");
                    break;
                }
                Err(TryRecvError::Empty) => {
                    debug!(
                        "No more data in the sync queue, task batch is not full, length: {}, max allowed size: {max_timelines_to_sync}",
                        batched_timelines.len()
                    );
                    break;
                }
            }
        }

        ControlFlow::Continue(tasks)
    }

    /// Length of the queue, assuming that all receiver counterparts were only called using the queue api.
    pub fn len() -> usize {
        LENGTH.load(Ordering::Relaxed)
    }
}

/// A task to run in the async download/upload loop.
/// Limited by the number of retries, after certain threshold the failing task gets evicted and the timeline disabled.
#[derive(Debug)]
pub enum SyncTask {
    /// A checkpoint outcome with possible local file updates that need actualization in the remote storage.
    /// Not necessary more fresh than the one already uploaded.
    Download(SyncData<TimelineDownload>),
    /// A certain amount of image files to download.
    Upload(SyncData<TimelineUpload>),
    /// Delete remote files.
    Delete(SyncData<TimelineDelete>),
}

/// Stores the data to synd and its retries, to evict the tasks failing to frequently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncData<T> {
    retries: u32,
    data: T,
}

impl<T> SyncData<T> {
    fn new(retries: u32, data: T) -> Self {
        Self { retries, data }
    }
}

impl SyncTask {
    fn download(download_task: TimelineDownload) -> Self {
        Self::Download(SyncData::new(0, download_task))
    }

    fn upload(upload_task: TimelineUpload) -> Self {
        Self::Upload(SyncData::new(0, upload_task))
    }

    fn delete(delete_task: TimelineDelete) -> Self {
        Self::Delete(SyncData::new(0, delete_task))
    }
}

#[derive(Debug, Default)]
struct SyncTaskBatch {
    upload: Option<SyncData<TimelineUpload>>,
    download: Option<SyncData<TimelineDownload>>,
    delete: Option<SyncData<TimelineDelete>>,
}

impl SyncTaskBatch {
    fn new(task: SyncTask) -> Self {
        let mut new_self = Self::default();
        new_self.add(task);
        new_self
    }

    fn add(&mut self, task: SyncTask) {
        match task {
            SyncTask::Download(new_download) => match &mut self.download {
                Some(batch_download) => {
                    batch_download.retries = batch_download.retries.min(new_download.retries);
                    batch_download
                        .data
                        .layers_to_skip
                        .extend(new_download.data.layers_to_skip.into_iter());
                }
                None => self.download = Some(new_download),
            },
            SyncTask::Upload(new_upload) => match &mut self.upload {
                Some(batch_upload) => {
                    batch_upload.retries = batch_upload.retries.min(new_upload.retries);

                    let batch_data = &mut batch_upload.data;
                    let new_data = new_upload.data;
                    batch_data
                        .layers_to_upload
                        .extend(new_data.layers_to_upload.into_iter());
                    batch_data
                        .uploaded_layers
                        .extend(new_data.uploaded_layers.into_iter());
                    if batch_data
                        .metadata
                        .as_ref()
                        .map(|meta| meta.disk_consistent_lsn())
                        <= new_data
                            .metadata
                            .as_ref()
                            .map(|meta| meta.disk_consistent_lsn())
                    {
                        batch_data.metadata = new_data.metadata;
                    }
                }
                None => self.upload = Some(new_upload),
            },
            SyncTask::Delete(new_delete) => match &mut self.delete {
                Some(batch_delete) => {
                    batch_delete.retries = batch_delete.retries.min(new_delete.retries);

                    batch_delete
                        .data
                        .layers_to_delete
                        .extend(new_delete.data.layers_to_delete.into_iter());
                    batch_delete
                        .data
                        .deleted_layers
                        .extend(new_delete.data.deleted_layers.into_iter());
                }
                None => self.delete = Some(new_delete),
            },
        }
    }
}

/// Local timeline files for upload, appeared after the new checkpoint.
/// Current checkpoint design assumes new files are added only, no deletions or amendment happens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelineUpload {
    /// Layer file path in the pageserver workdir, that were added for the corresponding checkpoint.
    layers_to_upload: HashSet<PathBuf>,
    /// Already uploaded layers. Used to store the data about the uploads between task retries
    /// and to record the data into the remote index after the task got completed or evicted.
    uploaded_layers: HashSet<PathBuf>,
    metadata: Option<TimelineMetadata>,
}

/// A timeline download task.
/// Does not contain the file list to download, to allow other
/// parts of the pageserer code to schedule the task
/// without using the remote index or any other ways to list the remote timleine files.
/// Skips the files that are already downloaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelineDownload {
    layers_to_skip: HashSet<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelineDelete {
    layers_to_delete: HashSet<PathBuf>,
    deleted_layers: HashSet<PathBuf>,
    deletion_registered: bool,
}

/// Adds the new checkpoint files as an upload sync task to the queue.
/// On task failure, it gets retried again from the start a number of times.
///
/// Ensure that the loop is started otherwise the task is never processed.
pub fn schedule_layer_upload(
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
    layers_to_upload: HashSet<PathBuf>,
    metadata: Option<TimelineMetadata>,
) {
    debug!("Scheduling layer upload for tenant {tenant_id}, timeline {timeline_id}, to upload: {layers_to_upload:?}");
    if !sync_queue::push(
        ZTenantTimelineId {
            tenant_id,
            timeline_id,
        },
        SyncTask::upload(TimelineUpload {
            layers_to_upload,
            uploaded_layers: HashSet::new(),
            metadata,
        }),
    ) {
        warn!("Could not send an upload task for tenant {tenant_id}, timeline {timeline_id}")
    } else {
        debug!("Upload task for tenant {tenant_id}, timeline {timeline_id} sent")
    }
}

pub fn schedule_layer_delete(
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
    layers_to_delete: HashSet<PathBuf>,
) {
    debug!("Scheduling layer deletion for tenant {tenant_id}, timeline {timeline_id}, to delete: {layers_to_delete:?}");
    if !sync_queue::push(
        ZTenantTimelineId {
            tenant_id,
            timeline_id,
        },
        SyncTask::delete(TimelineDelete {
            layers_to_delete,
            deleted_layers: HashSet::new(),
            deletion_registered: false,
        }),
    ) {
        warn!("Could not send deletion task for tenant {tenant_id}, timeline {timeline_id}")
    } else {
        debug!("Deletion task for tenant {tenant_id}, timeline {timeline_id} sent")
    }
}

/// Requests the download of the entire timeline for a given tenant.
/// No existing local files are currently overwritten, except the metadata file (if its disk_consistent_lsn is less than the downloaded one).
/// The metadata file is always updated last, to avoid inconsistencies.
///
/// On any failure, the task gets retried, omitting already downloaded layers.
///
/// Ensure that the loop is started otherwise the task is never processed.
pub fn schedule_layer_download(tenant_id: ZTenantId, timeline_id: ZTimelineId) {
    debug!("Scheduling layer download for tenant {tenant_id}, timeline {timeline_id}");
    sync_queue::push(
        ZTenantTimelineId {
            tenant_id,
            timeline_id,
        },
        SyncTask::download(TimelineDownload {
            layers_to_skip: HashSet::new(),
        }),
    );
}

/// Uses a remote storage given to start the storage sync loop.
/// See module docs for loop step description.
pub(super) fn spawn_storage_sync_thread<P, S>(
    conf: &'static PageServerConf,
    local_timeline_files: HashMap<ZTenantTimelineId, (TimelineMetadata, HashSet<PathBuf>)>,
    storage: S,
    max_concurrent_timelines_sync: NonZeroUsize,
    max_sync_errors: NonZeroU32,
) -> anyhow::Result<SyncStartupData>
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<RemoteObjectId = P> + Send + Sync + 'static,
{
    let (sender, receiver) = mpsc::unbounded_channel();
    sync_queue::init(sender)?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Failed to create storage sync runtime")?;

    let applicable_index_parts = runtime.block_on(try_fetch_index_parts(
        conf,
        &storage,
        local_timeline_files.keys().copied().collect(),
    ));

    let remote_index = RemoteIndex::from_parts(conf, applicable_index_parts)?;

    let local_timeline_init_statuses = schedule_first_sync_tasks(
        &mut runtime.block_on(remote_index.write()),
        local_timeline_files,
    );

    let loop_index = remote_index.clone();
    thread_mgr::spawn(
        ThreadKind::StorageSync,
        None,
        None,
        "Remote storage sync thread",
        false,
        move || {
            storage_sync_loop(
                runtime,
                conf,
                receiver,
                Arc::new(storage),
                loop_index,
                max_concurrent_timelines_sync,
                max_sync_errors,
            );
            Ok(())
        },
    )
    .context("Failed to spawn remote storage sync thread")?;
    Ok(SyncStartupData {
        remote_index,
        local_timeline_init_statuses,
    })
}

#[allow(clippy::too_many_arguments)]
fn storage_sync_loop<P, S>(
    runtime: Runtime,
    conf: &'static PageServerConf,
    mut receiver: UnboundedReceiver<(ZTenantTimelineId, SyncTask)>,
    storage: Arc<S>,
    index: RemoteIndex,
    max_concurrent_timelines_sync: NonZeroUsize,
    max_sync_errors: NonZeroU32,
) where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<RemoteObjectId = P> + Send + Sync + 'static,
{
    info!("Starting remote storage sync loop");
    loop {
        let loop_index = index.clone();
        let storage = Arc::clone(&storage);
        let loop_step = runtime.block_on(async {
            tokio::select! {
                step = loop_step(
                    conf,
                    &mut receiver,
                    storage,
                    loop_index,
                    max_concurrent_timelines_sync,
                    max_sync_errors,
                )
                .instrument(info_span!("storage_sync_loop_step")) => step,
                _ = thread_mgr::shutdown_watcher() => ControlFlow::Break(()),
            }
        });

        match loop_step {
            ControlFlow::Continue(new_timeline_states) => {
                if new_timeline_states.is_empty() {
                    debug!("Sync loop step completed, no new timeline states");
                } else {
                    info!(
                        "Sync loop step completed, {} new timeline state update(s)",
                        new_timeline_states.len()
                    );
                    // Batch timeline download registration to ensure that the external registration code won't block any running tasks before.
                    apply_timeline_sync_status_updates(conf, &index, new_timeline_states);
                }
            }
            ControlFlow::Break(()) => {
                info!("Shutdown requested, stopping");
                break;
            }
        }
    }
}

async fn loop_step<P, S>(
    conf: &'static PageServerConf,
    receiver: &mut UnboundedReceiver<(ZTenantTimelineId, SyncTask)>,
    storage: Arc<S>,
    index: RemoteIndex,
    max_concurrent_timelines_sync: NonZeroUsize,
    max_sync_errors: NonZeroU32,
) -> ControlFlow<(), HashMap<ZTenantId, HashMap<ZTimelineId, TimelineSyncStatusUpdate>>>
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<RemoteObjectId = P> + Send + Sync + 'static,
{
    let batched_tasks =
        match sync_queue::next_task_batch(receiver, max_concurrent_timelines_sync).await {
            ControlFlow::Continue(batch) => batch,
            ControlFlow::Break(()) => return ControlFlow::Break(()),
        };

    let remaining_queue_length = sync_queue::len();
    REMAINING_SYNC_ITEMS.set(remaining_queue_length as i64);
    if remaining_queue_length > 0 || !batched_tasks.is_empty() {
        info!("Processing tasks for {} timelines in batch, more tasks left to process: {remaining_queue_length}", batched_tasks.len());
    } else {
        debug!("No tasks to process");
        return ControlFlow::Continue(HashMap::new());
    }

    let mut sync_results = batched_tasks
        .into_iter()
        .map(|(sync_id, batch)| {
            let storage = Arc::clone(&storage);
            let index = index.clone();
            async move {
                let state_update =
                    process_sync_task_batch(conf, storage, index, max_sync_errors, sync_id, batch)
                        .instrument(info_span!("process_sync_task_batch", sync_id = %sync_id))
                        .await;
                (sync_id, state_update)
            }
        })
        .collect::<FuturesUnordered<_>>();

    let mut new_timeline_states: HashMap<
        ZTenantId,
        HashMap<ZTimelineId, TimelineSyncStatusUpdate>,
    > = HashMap::with_capacity(max_concurrent_timelines_sync.get());
    while let Some((sync_id, state_update)) = sync_results.next().await {
        debug!("Finished storage sync task for sync id {sync_id}");
        if let Some(state_update) = state_update {
            new_timeline_states
                .entry(sync_id.tenant_id)
                .or_default()
                .insert(sync_id.timeline_id, state_update);
        }
    }

    ControlFlow::Continue(new_timeline_states)
}

async fn process_sync_task_batch<P, S>(
    conf: &'static PageServerConf,
    storage: Arc<S>,
    index: RemoteIndex,
    max_sync_errors: NonZeroU32,
    sync_id: ZTenantTimelineId,
    batch: SyncTaskBatch,
) -> Option<TimelineSyncStatusUpdate>
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<RemoteObjectId = P> + Send + Sync + 'static,
{
    let sync_start = Instant::now();
    let current_remote_timeline = { index.read().await.timeline_entry(&sync_id).cloned() };

    let upload_data = batch.upload.clone();
    let download_data = batch.download.clone();
    let (upload_result, status_update) = tokio::join!(
        async {
            if let Some(upload_data) = upload_data {
                match validate_task_retries(upload_data, max_sync_errors)
                    .instrument(info_span!("retries_validation"))
                    .await
                {
                    ControlFlow::Continue(new_upload_data) => {
                        upload_timeline_data(
                            conf,
                            (storage.as_ref(), &index),
                            current_remote_timeline.as_ref(),
                            sync_id,
                            new_upload_data,
                            sync_start,
                            "upload",
                        )
                        .await;
                        return Some(());
                    }
                    ControlFlow::Break(failed_upload_data) => {
                        if let Err(e) = update_remote_data(
                            conf,
                            storage.as_ref(),
                            &index,
                            sync_id,
                            RemoteDataUpdate::Upload {
                                uploaded_data: failed_upload_data.data,
                                upload_failed: true,
                            },
                        )
                        .await
                        {
                            error!("Failed to update remote timeline {sync_id}: {e:?}");
                        }
                    }
                }
            }
            None
        }
        .instrument(info_span!("upload_timeline_data")),
        async {
            if let Some(download_data) = download_data {
                match validate_task_retries(download_data, max_sync_errors)
                    .instrument(info_span!("retries_validation"))
                    .await
                {
                    ControlFlow::Continue(new_download_data) => {
                        return download_timeline_data(
                            conf,
                            (storage.as_ref(), &index),
                            current_remote_timeline.as_ref(),
                            sync_id,
                            new_download_data,
                            sync_start,
                            "download",
                        )
                        .await
                    }
                    ControlFlow::Break(_) => {
                        index
                            .write()
                            .await
                            .set_awaits_download(&sync_id, false)
                            .ok();
                    }
                }
            }
            None
        }
        .instrument(info_span!("download_timeline_data")),
    );

    if let Some(delete_data) = batch.delete {
        match validate_task_retries(delete_data, max_sync_errors)
            .instrument(info_span!("retries_validation"))
            .await
        {
            ControlFlow::Continue(new_delete_data) => {
                delete_timeline_data(
                    conf,
                    (storage.as_ref(), &index),
                    sync_id,
                    new_delete_data,
                    sync_start,
                    "delete",
                )
                .instrument(info_span!("delete_timeline_data"))
                .await;
            }
            ControlFlow::Break(failed_delete_data) => {
                if let Err(e) = update_remote_data(
                    conf,
                    storage.as_ref(),
                    &index,
                    sync_id,
                    RemoteDataUpdate::Delete(&failed_delete_data.data.deleted_layers),
                )
                .await
                {
                    error!("Failed to update remote timeline {sync_id}: {e:?}");
                }
            }
        }
    }

    status_update
}

async fn download_timeline_data<P, S>(
    conf: &'static PageServerConf,
    (storage, index): (&S, &RemoteIndex),
    current_remote_timeline: Option<&RemoteTimeline>,
    sync_id: ZTenantTimelineId,
    new_download_data: SyncData<TimelineDownload>,
    sync_start: Instant,
    task_name: &str,
) -> Option<TimelineSyncStatusUpdate>
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<RemoteObjectId = P> + Send + Sync + 'static,
{
    match download_timeline_layers(
        conf,
        storage,
        current_remote_timeline,
        sync_id,
        new_download_data,
    )
    .await
    {
        DownloadedTimeline::Abort => {
            register_sync_status(sync_start, task_name, None);
            if let Err(e) = index.write().await.set_awaits_download(&sync_id, false) {
                error!("Timeline {sync_id} was expected to be in the remote index after a download attempt, but it's absent: {e:?}");
            }
        }
        DownloadedTimeline::FailedAndRescheduled => {
            register_sync_status(sync_start, task_name, Some(false));
        }
        DownloadedTimeline::Successful(mut download_data) => {
            match update_local_metadata(conf, sync_id, current_remote_timeline).await {
                Ok(()) => match index.write().await.set_awaits_download(&sync_id, false) {
                    Ok(()) => {
                        register_sync_status(sync_start, task_name, Some(true));
                        return Some(TimelineSyncStatusUpdate::Downloaded);
                    }
                    Err(e) => {
                        error!("Timeline {sync_id} was expected to be in the remote index after a sucessful download, but it's absent: {e:?}");
                    }
                },
                Err(e) => {
                    error!("Failed to update local timeline metadata: {e:?}");
                    download_data.retries += 1;
                    sync_queue::push(sync_id, SyncTask::Download(download_data));
                    register_sync_status(sync_start, task_name, Some(false));
                }
            }
        }
    }

    None
}

async fn update_local_metadata(
    conf: &'static PageServerConf,
    sync_id: ZTenantTimelineId,
    remote_timeline: Option<&RemoteTimeline>,
) -> anyhow::Result<()> {
    let remote_metadata = match remote_timeline {
        Some(timeline) => &timeline.metadata,
        None => {
            info!("No remote timeline to update local metadata from, skipping the update");
            return Ok(());
        }
    };
    let remote_lsn = remote_metadata.disk_consistent_lsn();

    let local_metadata_path = metadata_path(conf, sync_id.timeline_id, sync_id.tenant_id);
    let local_lsn = if local_metadata_path.exists() {
        let local_metadata = read_metadata_file(&local_metadata_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to load local metadata from path '{}'",
                    local_metadata_path.display()
                )
            })?;

        Some(local_metadata.disk_consistent_lsn())
    } else {
        None
    };

    if local_lsn < Some(remote_lsn) {
        info!("Updating local timeline metadata from remote timeline: local disk_consistent_lsn={local_lsn:?}, remote disk_consistent_lsn={remote_lsn}");
        // clone because spawn_blocking requires static lifetime
        let cloned_metadata = remote_metadata.to_owned();
        let ZTenantTimelineId {
            tenant_id,
            timeline_id,
        } = sync_id;
        tokio::task::spawn_blocking(move || {
            LayeredRepository::save_metadata(conf, timeline_id, tenant_id, &cloned_metadata, true)
        })
        .await
        .with_context(|| {
            format!(
                "failed to join save_metadata task for {}",
                local_metadata_path.display()
            )
        })?
        .with_context(|| {
            format!(
                "Failed to write remote metadata bytes locally to path '{}'",
                local_metadata_path.display()
            )
        })?;
    } else {
        info!("Local metadata at path '{}' has later disk consistent Lsn ({local_lsn:?}) than the remote one ({remote_lsn}), skipping the update", local_metadata_path.display());
    }

    Ok(())
}

async fn delete_timeline_data<P, S>(
    conf: &'static PageServerConf,
    (storage, index): (&S, &RemoteIndex),
    sync_id: ZTenantTimelineId,
    mut new_delete_data: SyncData<TimelineDelete>,
    sync_start: Instant,
    task_name: &str,
) where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
{
    let timeline_delete = &mut new_delete_data.data;

    if !timeline_delete.deletion_registered {
        if let Err(e) = update_remote_data(
            conf,
            storage,
            index,
            sync_id,
            RemoteDataUpdate::Delete(&timeline_delete.layers_to_delete),
        )
        .await
        {
            error!("Failed to update remote timeline {sync_id}: {e:?}");
            new_delete_data.retries += 1;
            sync_queue::push(sync_id, SyncTask::Delete(new_delete_data));
            register_sync_status(sync_start, task_name, Some(false));
            return;
        }
    }
    timeline_delete.deletion_registered = true;

    let sync_status = delete_timeline_layers(storage, sync_id, new_delete_data).await;
    register_sync_status(sync_start, task_name, Some(sync_status));
}

async fn read_metadata_file(metadata_path: &Path) -> anyhow::Result<TimelineMetadata> {
    TimelineMetadata::from_bytes(
        &fs::read(metadata_path)
            .await
            .context("Failed to read local metadata bytes from fs")?,
    )
    .context("Failed to parse metadata bytes")
}

async fn upload_timeline_data<P, S>(
    conf: &'static PageServerConf,
    (storage, index): (&S, &RemoteIndex),
    current_remote_timeline: Option<&RemoteTimeline>,
    sync_id: ZTenantTimelineId,
    new_upload_data: SyncData<TimelineUpload>,
    sync_start: Instant,
    task_name: &str,
) where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<RemoteObjectId = P> + Send + Sync + 'static,
{
    let mut uploaded_data =
        match upload_timeline_layers(storage, current_remote_timeline, sync_id, new_upload_data)
            .await
        {
            UploadedTimeline::FailedAndRescheduled => {
                register_sync_status(sync_start, task_name, Some(false));
                return;
            }
            UploadedTimeline::Successful(upload_data) => upload_data,
            UploadedTimeline::SuccessfulAfterLocalFsUpdate(mut outdated_upload_data) => {
                if outdated_upload_data.data.metadata.is_some() {
                    let local_metadata_path =
                        metadata_path(conf, sync_id.timeline_id, sync_id.tenant_id);
                    let local_metadata = match read_metadata_file(&local_metadata_path).await {
                        Ok(metadata) => metadata,
                        Err(e) => {
                            error!(
                                "Failed to load local metadata from path '{}': {e:?}",
                                local_metadata_path.display()
                            );
                            outdated_upload_data.retries += 1;
                            sync_queue::push(sync_id, SyncTask::Upload(outdated_upload_data));
                            register_sync_status(sync_start, task_name, Some(false));
                            return;
                        }
                    };

                    outdated_upload_data.data.metadata = Some(local_metadata);
                }
                outdated_upload_data
            }
        };

    match update_remote_data(
        conf,
        storage,
        index,
        sync_id,
        RemoteDataUpdate::Upload {
            uploaded_data: uploaded_data.data.clone(),
            upload_failed: false,
        },
    )
    .await
    {
        Ok(()) => {
            register_sync_status(sync_start, task_name, Some(true));
        }
        Err(e) => {
            error!("Failed to update remote timeline {sync_id}: {e:?}");
            uploaded_data.retries += 1;
            sync_queue::push(sync_id, SyncTask::Upload(uploaded_data));
            register_sync_status(sync_start, task_name, Some(false));
        }
    }
}

enum RemoteDataUpdate<'a> {
    Upload {
        uploaded_data: TimelineUpload,
        upload_failed: bool,
    },
    Delete(&'a HashSet<PathBuf>),
}

async fn update_remote_data<P, S>(
    conf: &'static PageServerConf,
    storage: &S,
    index: &RemoteIndex,
    sync_id: ZTenantTimelineId,
    update: RemoteDataUpdate<'_>,
) -> anyhow::Result<()>
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<RemoteObjectId = P> + Send + Sync + 'static,
{
    info!("Updating remote index for the timeline");
    let updated_remote_timeline = {
        let mut index_accessor = index.write().await;

        match index_accessor.timeline_entry_mut(&sync_id) {
            Some(existing_entry) => {
                match update {
                    RemoteDataUpdate::Upload {
                        uploaded_data,
                        upload_failed,
                    } => {
                        if let Some(new_metadata) = uploaded_data.metadata.as_ref() {
                            if existing_entry.metadata.disk_consistent_lsn()
                                < new_metadata.disk_consistent_lsn()
                            {
                                existing_entry.metadata = new_metadata.clone();
                            }
                        }
                        if upload_failed {
                            existing_entry.add_upload_failures(
                                uploaded_data.layers_to_upload.iter().cloned(),
                            );
                        } else {
                            existing_entry
                                .add_timeline_layers(uploaded_data.uploaded_layers.iter().cloned());
                        }
                    }
                    RemoteDataUpdate::Delete(layers_to_remove) => {
                        existing_entry.remove_layers(layers_to_remove)
                    }
                }
                existing_entry.clone()
            }
            None => match update {
                RemoteDataUpdate::Upload {
                    uploaded_data,
                    upload_failed,
                } => {
                    let new_metadata = match uploaded_data.metadata.as_ref() {
                        Some(new_metadata) => new_metadata,
                        None => bail!("For timeline {sync_id} upload, there's no upload metadata and no remote index entry, cannot create a new one"),
                    };
                    let mut new_remote_timeline = RemoteTimeline::new(new_metadata.clone());
                    if upload_failed {
                        new_remote_timeline
                            .add_upload_failures(uploaded_data.layers_to_upload.iter().cloned());
                    } else {
                        new_remote_timeline
                            .add_timeline_layers(uploaded_data.uploaded_layers.iter().cloned());
                    }

                    index_accessor.add_timeline_entry(sync_id, new_remote_timeline.clone());
                    new_remote_timeline
                }
                RemoteDataUpdate::Delete(_) => {
                    warn!("No remote index entry for timeline {sync_id}, skipping deletion");
                    return Ok(());
                }
            },
        }
    };

    let timeline_path = conf.timeline_path(&sync_id.timeline_id, &sync_id.tenant_id);
    let new_index_part =
        IndexPart::from_remote_timeline(&timeline_path, updated_remote_timeline)
            .context("Failed to create an index part from the updated remote timeline")?;

    info!("Uploading remote data for the timeline");
    upload_index_part(conf, storage, sync_id, new_index_part)
        .await
        .context("Failed to upload new index part")
}

async fn validate_task_retries<T>(
    sync_data: SyncData<T>,
    max_sync_errors: NonZeroU32,
) -> ControlFlow<SyncData<T>, SyncData<T>> {
    let current_attempt = sync_data.retries;
    let max_sync_errors = max_sync_errors.get();
    if current_attempt >= max_sync_errors {
        error!(
            "Aborting task that failed {current_attempt} times, exceeding retries threshold of {max_sync_errors}",
        );
        return ControlFlow::Break(sync_data);
    }

    if current_attempt > 0 {
        let seconds_to_wait = 2.0_f64.powf(current_attempt as f64 - 1.0).min(30.0);
        info!("Waiting {seconds_to_wait} seconds before starting the task");
        tokio::time::sleep(Duration::from_secs_f64(seconds_to_wait)).await;
    }
    ControlFlow::Continue(sync_data)
}

async fn try_fetch_index_parts<P, S>(
    conf: &'static PageServerConf,
    storage: &S,
    keys: HashSet<ZTenantTimelineId>,
) -> HashMap<ZTenantTimelineId, IndexPart>
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<RemoteObjectId = P> + Send + Sync + 'static,
{
    let mut index_parts = HashMap::with_capacity(keys.len());

    let mut part_downloads = keys
        .into_iter()
        .map(|id| async move { (id, download_index_part(conf, storage, id).await) })
        .collect::<FuturesUnordered<_>>();

    while let Some((id, part_upload_result)) = part_downloads.next().await {
        match part_upload_result {
            Ok(index_part) => {
                debug!("Successfully fetched index part for {id}");
                index_parts.insert(id, index_part);
            }
            Err(e) => warn!("Failed to fetch index part for {id}: {e}"),
        }
    }

    index_parts
}

fn schedule_first_sync_tasks(
    index: &mut RemoteTimelineIndex,
    local_timeline_files: HashMap<ZTenantTimelineId, (TimelineMetadata, HashSet<PathBuf>)>,
) -> LocalTimelineInitStatuses {
    let mut local_timeline_init_statuses = LocalTimelineInitStatuses::new();

    let mut new_sync_tasks =
        VecDeque::with_capacity(local_timeline_files.len().max(local_timeline_files.len()));

    for (sync_id, (local_metadata, local_files)) in local_timeline_files {
        match index.timeline_entry_mut(&sync_id) {
            Some(remote_timeline) => {
                let (timeline_status, awaits_download) = compare_local_and_remote_timeline(
                    &mut new_sync_tasks,
                    sync_id,
                    local_metadata,
                    local_files,
                    remote_timeline,
                );
                let was_there = local_timeline_init_statuses
                    .entry(sync_id.tenant_id)
                    .or_default()
                    .insert(sync_id.timeline_id, timeline_status);

                if was_there.is_some() {
                    // defensive check
                    warn!(
                        "Overwriting timeline init sync status. Status {timeline_status:?}, timeline {}",
                        sync_id.timeline_id
                    );
                }
                remote_timeline.awaits_download = awaits_download;
            }
            None => {
                // TODO (rodionov) does this mean that we've crashed during tenant creation?
                //  is it safe to upload this checkpoint? could it be half broken?
                new_sync_tasks.push_back((
                    sync_id,
                    SyncTask::upload(TimelineUpload {
                        layers_to_upload: local_files,
                        uploaded_layers: HashSet::new(),
                        metadata: Some(local_metadata),
                    }),
                ));
                local_timeline_init_statuses
                    .entry(sync_id.tenant_id)
                    .or_default()
                    .insert(
                        sync_id.timeline_id,
                        LocalTimelineInitStatus::LocallyComplete,
                    );
            }
        }
    }

    new_sync_tasks.into_iter().for_each(|(sync_id, task)| {
        sync_queue::push(sync_id, task);
    });
    local_timeline_init_statuses
}

fn compare_local_and_remote_timeline(
    new_sync_tasks: &mut VecDeque<(ZTenantTimelineId, SyncTask)>,
    sync_id: ZTenantTimelineId,
    local_metadata: TimelineMetadata,
    local_files: HashSet<PathBuf>,
    remote_entry: &RemoteTimeline,
) -> (LocalTimelineInitStatus, bool) {
    let remote_files = remote_entry.stored_files();

    // TODO probably here we need more sophisticated logic,
    //   if more data is available remotely can we just download whats there?
    //   without trying to upload something. It may be tricky, needs further investigation.
    //   For now looks strange that we can request upload
    //   and dowload for the same timeline simultaneously.
    //   (upload needs to be only for previously unsynced files, not whole timeline dir).
    //   If one of the tasks fails they will be reordered in the queue which can lead
    //   to timeline being stuck in evicted state
    let number_of_layers_to_download = remote_files.difference(&local_files).count();
    let (initial_timeline_status, awaits_download) = if number_of_layers_to_download > 0 {
        new_sync_tasks.push_back((
            sync_id,
            SyncTask::download(TimelineDownload {
                layers_to_skip: local_files.clone(),
            }),
        ));
        (LocalTimelineInitStatus::NeedsSync, true)
        // we do not need to manupulate with remote consistent lsn here
        // because it will be updated when sync will be completed
    } else {
        (LocalTimelineInitStatus::LocallyComplete, false)
    };

    let layers_to_upload = local_files
        .difference(remote_files)
        .cloned()
        .collect::<HashSet<_>>();
    if !layers_to_upload.is_empty() {
        new_sync_tasks.push_back((
            sync_id,
            SyncTask::upload(TimelineUpload {
                layers_to_upload,
                uploaded_layers: HashSet::new(),
                metadata: Some(local_metadata),
            }),
        ));
        // Note that status here doesn't change.
    }

    (initial_timeline_status, awaits_download)
}

fn register_sync_status(sync_start: Instant, sync_name: &str, sync_status: Option<bool>) {
    let secs_elapsed = sync_start.elapsed().as_secs_f64();
    info!("Processed a sync task in {secs_elapsed:.2} seconds");
    match sync_status {
        Some(true) => IMAGE_SYNC_TIME.with_label_values(&[sync_name, "success"]),
        Some(false) => IMAGE_SYNC_TIME.with_label_values(&[sync_name, "failure"]),
        None => return,
    }
    .observe(secs_elapsed)
}

#[cfg(test)]
mod test_utils {
    use utils::lsn::Lsn;

    use crate::repository::repo_harness::RepoHarness;

    use super::*;

    pub async fn create_local_timeline(
        harness: &RepoHarness<'_>,
        timeline_id: ZTimelineId,
        filenames: &[&str],
        metadata: TimelineMetadata,
    ) -> anyhow::Result<TimelineUpload> {
        let timeline_path = harness.timeline_path(&timeline_id);
        fs::create_dir_all(&timeline_path).await?;

        let mut layers_to_upload = HashSet::with_capacity(filenames.len());
        for &file in filenames {
            let file_path = timeline_path.join(file);
            fs::write(&file_path, dummy_contents(file).into_bytes()).await?;
            layers_to_upload.insert(file_path);
        }

        fs::write(
            metadata_path(harness.conf, timeline_id, harness.tenant_id),
            metadata.to_bytes()?,
        )
        .await?;

        Ok(TimelineUpload {
            layers_to_upload,
            uploaded_layers: HashSet::new(),
            metadata: Some(metadata),
        })
    }

    pub fn dummy_contents(name: &str) -> String {
        format!("contents for {name}")
    }

    pub fn dummy_metadata(disk_consistent_lsn: Lsn) -> TimelineMetadata {
        TimelineMetadata::new(disk_consistent_lsn, None, None, Lsn(0), Lsn(0), Lsn(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batching_tests() {
        todo!("TODO kb")
    }
}
