use clap::Parser;

#[derive(Parser)]
pub(crate) enum JobsAction {
    /// Show job queue health summary
    Status {
        /// Output as JSON instead of human-readable text
        #[arg(long)]
        json: bool,
        /// Route through brain-daemon over the local Unix socket
        #[arg(long)]
        remote: bool,
    },
    /// Retry a failed or stuck job by resetting it to pending
    Retry {
        /// The job ID to retry
        job_id: String,
        /// Route through brain-daemon over the local Unix socket (default: true)
        #[arg(long, default_value = "true")]
        remote: bool,
    },
    /// Run garbage collection on completed jobs
    Gc {
        /// Delete completed jobs older than this many days
        #[arg(long, default_value = "7")]
        older_than_days: u32,
        /// Route through brain-daemon over the local Unix socket (default: true)
        #[arg(long, default_value = "true")]
        remote: bool,
    },
}
