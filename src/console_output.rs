use colored::Colorize;
use std::io;
use std::io::Write;
use term_size::*;

use crate::progress_message::*;
use crate::rsync::*;

/// Note: Credit to these functions goes to dmerejkowsky's rusync

pub struct ConsoleProgressOutput {}

impl ConsoleProgressOutput {
    pub fn new() -> ConsoleProgressOutput {
        ConsoleProgressOutput {}
    }
}

impl ProgressInfo for ConsoleProgressOutput {
    fn done_syncing(&self) {
        erase_line();
    }

    fn start(&self, source: &str, destination: &str) {
        println!(
            "{} Syncing from {} to {} …",
            "::".color("blue"),
            source.bold(),
            destination.bold()
        )
    }

    fn new_file(&self, _name: &str) {}

    fn progress(&self, progress: &Progress) {
        let eta_str = human_seconds(progress.eta);
        let percent_width = 3;
        let eta_width = eta_str.len();
        let index = progress.index;
        let index_width = index.to_string().len();
        let num_files = progress.num_files;
        let num_files_width = num_files.to_string().len();
        let widgets_width = percent_width + index_width + num_files_width + eta_width;
        let num_separators = 5;
        let line_width = get_terminal_width();
        let file_width = line_width - widgets_width - num_separators - 1;
        let mut current_file = progress.current_file.clone();
        current_file.truncate(file_width as usize);
        let current_file = format!(
            "{filename:<pad$}",
            pad = file_width as usize,
            filename = current_file
        );
        let file_percent = if progress.file_size == 0 {
            100
        } else {
            ((progress.file_done * 100) as usize) / progress.file_size
        };
        print!(
            "{:>3}% {}/{} {} {:<}\r",
            file_percent, index, num_files, current_file, eta_str
        );
        let _ = io::stdout().flush();
    }

    fn end(&self, stats: &SyncStats) {
        println!(
            "{} Synced {} files ({} up to date)",
            " ✓".color("green"),
            stats.num_synced,
            stats.up_to_date
        );
        println!(
            "{} files copied, {} symlinks created, {} symlinks updated, {} symlinks skipped.",
            stats.copied, stats.symlink_created, stats.symlink_updated, stats.symlink_skipped
        );
        println!(
            "{} directories created, {} directories updated",
            stats.directory_created, stats.directory_updated
        );
        println!(
            "{} permissions updated, {} checksum updated",
            stats.permissions_update, stats.checksum_updated
        );
    }
}

fn get_terminal_width() -> usize {
    if let Some((w, _)) = dimensions() {
        return w;
    }
    //otherwise default to like, 80 characters
    80
}

fn erase_line() {
    let line_width = get_terminal_width();
    let line = vec![32 as u8; line_width as usize];
    print!("{}\r", String::from_utf8_lossy(&line));
}

fn human_seconds(s: usize) -> String {
    let hours = s / 3600;
    let minutes = (s / 60) % 60;
    let seconds = s % 60;
    return format!("{:02}:{:02}:{:02}", hours, minutes, seconds);
}
