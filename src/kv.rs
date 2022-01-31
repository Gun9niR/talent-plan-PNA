use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs::{create_dir_all, read_dir, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Take, Write};
use std::path::{Path, PathBuf};
use std::{fs, io};

use serde::{Deserialize, Serialize};

use crate::{KvsError, Result};

/// The maximum size of
const COMPACTION_THRESHOLD: u64 = 4 * 1024 * 1024;
const COMPACTION_MARK: char = '_';

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in on-disk log files in json format, for human readability.
///
/// Example:
///
/// ```rust
/// # use kvs::{KvStore, Result};
/// # fn try_main() -> Result<()> {
/// use std::env::current_dir;
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let val = store.get("key".to_owned())?;
/// assert_eq!(val, Some("value".to_owned()));
/// # Ok(())
/// # }

pub struct KvStore {
    /// The directory to store log files.
    path: PathBuf,
    /// Current generation number. Indicates which log file to append to currently.
    current_gen: u64,
    /// The set of generations that result from compaction.
    compacted_gen: HashSet<u64>,
    /// Map generation number to file reader.
    readers: HashMap<u64, BufReader<File>>,
    /// There is only one writer, because write requests always append command to the newest
    /// generation.
    writer: BufWriter<File>,
    /// In-memory index, map key to the log file and log pointer.
    /// Currently, range query is not supported, so hash map is quicker.
    index: HashMap<String, CommandPos>,
    /// The number of bytes in log that has not been compacted. When it reaches a threshold,
    /// compaction is triggered, remove stale log records.
    uncompacted_size: u64,
}

impl KvStore {
    /// Opens a `KvStore` with the given path.
    ///
    /// This will create a new directory if the given one does not exist.
    /// The logs will be scanned to rebuild the index.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        create_dir_all(&path)?;

        // Find the generations of existing logs, and determine the latest generation number.
        let sorted_gen = KvStore::get_log_gen(&path)?;
        let mut compacted_gen = HashSet::new();
        // Keep track of all the generations that result from compaction.
        for (compacted, gen) in &sorted_gen {
            if *compacted {
                compacted_gen.insert(*gen);
            }
        }
        let cur_gen = match sorted_gen.last() {
            Some((compacted, gen)) => {
                if *compacted {
                    *gen + 1
                } else {
                    *gen
                }
            }
            None => 1,
        };

        // Create file readers for all log files.
        let (uncompacted_size, mut readers) = KvStore::create_file_readers(&path, &sorted_gen)?;

        // Create index from previous log files.
        let index = KvStore::build_index(&sorted_gen, &mut readers)?;

        // If there are no log files currently, create one. Otherwise open the log ile with
        // largest generation number for writing.
        let writer = BufWriter::new(KvStore::new_log_file(&path, cur_gen, false)?);
        if sorted_gen.is_empty() {
            readers.insert(
                cur_gen,
                KvStore::create_file_reader(&path, &cur_gen, false)?,
            );
        }

        Ok(KvStore {
            path,
            current_gen: cur_gen,
            compacted_gen,
            readers,
            writer,
            index,
            uncompacted_size,
        })
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let kk = key.clone();
        let k = val.clone();
        // Create write command.
        let set_cmd = Command::Set {
            key: key.clone(),
            value: val,
        };

        // Persist the command to log file.
        let before_set_pos = self.writer.seek(SeekFrom::End(0))?;
        serde_json::to_writer(&mut self.writer, &set_cmd)?;
        self.writer.flush()?;
        let cmd_len = self.writer.stream_position()? - before_set_pos;

        // Update in-memory index.
        self.index.insert(
            key,
            CommandPos {
                gen: self.current_gen,
                pos: before_set_pos,
                len: cmd_len,
            },
        );

        // Do compaction if log size exceeds threshold.
        self.uncompacted_size += cmd_len;
        if self.uncompacted_size > COMPACTION_THRESHOLD {
            println!("compact at key {}, value {}", kk, k);
            self.compact()?;
        }
        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        // Key found.
        if let Some(cmd_pos) = self.index.get(key.as_str()) {
            let reader = self
                .readers
                .get_mut(&cmd_pos.gen)
                .expect(format!("Cannot find log reader {}", cmd_pos.gen).as_str());

            if reader.stream_position()? != cmd_pos.pos {
                reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            }

            let single_cmd_reader = KvStore::single_cmd_reader(&mut self.readers, cmd_pos)?;

            if let Command::Set { value, .. } = serde_json::from_reader(single_cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(KvsError::KeyNotFound)
            }
        }
        // Key not found.
        else {
            Ok(None)
        }
    }

    /// Remove a given key. First append the `Command::Remove` log, then remove from index.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(key.as_str()) {
            // Create remove command.
            let remove_cmd = Command::Remove { key: key.clone() };
            // Persist the log command to file.
            serde_json::to_writer(&mut self.writer, &remove_cmd)?;
            self.writer.flush()?;
            // Remove the command from in-memory index.
            let cmd_removed = self.index.remove(key.as_str()).unwrap();

            // Do compaction.
            self.uncompacted_size += cmd_removed.len;
            if self.uncompacted_size > COMPACTION_THRESHOLD {
                self.compact()?;
            }

            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    /// Return the adapted reader to be passed into `serde_json::from_reader`, which will return a
    /// `kv::Command` instance.
    fn single_cmd_reader<'a>(
        readers: &'a mut HashMap<u64, BufReader<File>>,
        cmd_pos: &'a CommandPos,
    ) -> Result<Take<&'a mut BufReader<File>>> {
        let reader = readers
            .get_mut(&cmd_pos.gen)
            .expect(format!("Cannot find log reader {}", cmd_pos.gen).as_str());

        if reader.stream_position()? != cmd_pos.pos {
            reader.seek(SeekFrom::Start(cmd_pos.pos))?;
        }

        Ok(reader.take(cmd_pos.len))
    }

    /// Scan the directory `path`, find the generations of all log files,
    /// and which of them are compacted.
    ///
    /// Returns a vector of tuple, where the first element indicates if the log file
    /// has been compacted, and the second element indicates the generation number of the log.
    fn get_log_gen(dir: &Path) -> Result<Vec<(bool, u64)>> {
        let mut vec_log_id: Vec<(bool, u64)> = read_dir(&dir)?
            .into_iter()
            .map(|e| e.unwrap().path())
            .filter(|path| {
                path.is_file() && path.extension().and_then(OsStr::to_str) == Some("log")
            })
            .filter_map(|path| {
                path.file_name()
                    .and_then(OsStr::to_str)
                    .map(|s| s.trim_end_matches(".log"))
                    .map(|s| {
                        if s.starts_with(COMPACTION_MARK) {
                            (true, s.trim_start_matches(COMPACTION_MARK).parse())
                        } else {
                            (false, s.parse())
                        }
                    })
            })
            .filter(|(.., res)| res.is_ok())
            .map(|(b, res)| (b, res.unwrap()))
            .collect();

        vec_log_id.sort_unstable_by(|a, b| a.1.cmp(&b.1));
        Ok(vec_log_id)
    }

    fn create_file_readers(
        dir: &Path,
        sorted_gen: &Vec<(bool, u64)>,
    ) -> Result<(u64, HashMap<u64, BufReader<File>>)> {
        let mut uncompacted_size = 0;
        let mut readers: HashMap<u64, BufReader<File>> = HashMap::new();
        for (compacted, gen) in sorted_gen {
            let reader = KvStore::create_file_reader(dir, gen, *compacted)?;
            if *compacted {
                uncompacted_size += reader.get_ref().metadata().unwrap().len();
            }
            readers.insert(*gen, reader);
        }
        Ok((uncompacted_size, readers))
    }

    fn create_file_reader(dir: &Path, gen: &u64, compacted: bool) -> Result<BufReader<File>> {
        Ok(BufReader::new(File::open(
            dir.join(KvStore::log_file_full_path(dir, *gen, compacted)),
        )?))
    }

    /// Create a new log file for **writing** in the directory `dir`, whose generation number is `gen`.
    fn new_log_file(dir: &Path, gen: u64, compacted: bool) -> Result<File> {
        Ok(OpenOptions::new()
            .read(false)
            .append(true)
            .create(true)
            .open(KvStore::log_file_full_path(dir, gen, compacted).as_path())?)
    }

    /// Build index from existing log files. Each entry in the index is a `CommandPos` struct.
    fn build_index(
        sorted_gen: &Vec<(bool, u64)>,
        readers: &mut HashMap<u64, BufReader<File>>,
    ) -> Result<HashMap<String, CommandPos>> {
        let mut index = HashMap::new();

        for (.., gen) in sorted_gen {
            let reader = readers.get_mut(gen).unwrap();
            let mut cur_pos = reader.seek(SeekFrom::Start(0))?;
            let mut stream = serde_json::Deserializer::from_reader(reader).into_iter::<Command>();
            while let Some(deserialize_res) = stream.next() {
                let new_pos = stream.byte_offset() as u64;
                match deserialize_res? {
                    Command::Set { key, .. } => index.insert(
                        key,
                        CommandPos {
                            gen: *gen,
                            pos: cur_pos,
                            len: new_pos - cur_pos,
                        },
                    ),
                    Command::Remove { key } => index.remove(&key),
                };
                cur_pos = new_pos;
            }
        }

        Ok(index)
    }

    /// Compact the logs. Two new log files are created, one for compaction result, another for
    /// write or remove commands.
    ///
    /// Using two files can avoid blocking `set` or `rm` commands during compaction.
    fn compact(&mut self) -> Result<()> {
        eprintln!("compact");
        let path_ref = self.path.as_path();
        let compaction_gen = self.current_gen + 1;
        self.current_gen = self.current_gen + 2;

        self.compacted_gen.insert(compaction_gen);

        let mut compaction_writer =
            BufWriter::new(KvStore::new_log_file(path_ref, compaction_gen, true)?);

        // At this point, write request are disabled.

        // Update writer, so that new logs can be written into the new log file.
        self.uncompacted_size = 0;
        self.writer = BufWriter::new(KvStore::new_log_file(path_ref, self.current_gen, false)?);

        // Update reader for the new log file so that new operations after the compaction is
        // triggered can be read.
        self.readers.insert(
            self.current_gen,
            KvStore::create_file_reader(path_ref, &self.current_gen, false)?,
        );

        // At this point write and remove requests can be served.

        let mut compaction_pos = 0;
        for cmd_pos in self.index.values_mut() {
            // Copy log entry.
            let mut cmd_reader = KvStore::single_cmd_reader(&mut self.readers, cmd_pos)?;
            let cmd_len = cmd_reader.limit();
            io::copy(&mut cmd_reader, &mut compaction_writer)?;
            // Update index.
            cmd_pos.gen = compaction_gen;
            cmd_pos.pos = compaction_pos;
            cmd_pos.len = cmd_len;
            compaction_pos += cmd_len;
        }

        // At this point stale log readers should transition to the reader of compaction file.
        // Read requests become unavailable.

        // Update reader for the compaction file.
        self.readers.insert(
            compaction_gen,
            KvStore::create_file_reader(self.path.as_path(), &compaction_gen, true)?,
        );
        let stale_gen: Vec<u64> = self
            .readers
            .keys()
            .filter(|gen| gen < &&compaction_gen)
            .cloned()
            .collect();
        // Remove stale log readers.
        for gen in stale_gen {
            if let Some(..) = self.readers.remove(&gen) {
                let compacted = self.compacted_gen.contains(&gen);
                if compacted {
                    self.compacted_gen.remove(&gen);
                }
                fs::remove_file(KvStore::log_file_full_path(path_ref, gen, compacted))?;
            };
        }
        // Read request become available again.

        Ok(())
    }

    #[inline(always)]
    fn log_file_full_path(dir: &Path, gen: u64, compacted: bool) -> PathBuf {
        let compaction_mark = if compacted { "_" } else { "" };
        dir.join(format!("{}{}.log", compaction_mark, gen))
    }
}

#[derive(Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

#[derive(Debug)]
struct CommandPos {
    gen: u64,
    pos: u64,
    len: u64,
}
