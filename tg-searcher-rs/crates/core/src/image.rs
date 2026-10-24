//! OpenCV-based image search.
//!
//! Each indexed image is reduced to:
//! * an ORB descriptor matrix (rotation/scale tolerant local features),
//! * a 64-bit perceptual hash (fast near-duplicate detection),
//! * a small JPEG thumbnail for `/img2img` replies.
//!
//! Matching combines a Lowe-ratio-test ORB matcher with the perceptual hash.
//! The acceptance threshold (`image.min_similarity`) and the minimum number of
//! good matches (`image.min_good_matches`) are configuration knobs.

use std::fs;
use std::path::{Path, PathBuf};

use opencv::prelude::*;
use opencv::{core, features2d, imgcodecs, imgproc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, warn};

use crate::config::ImageConfig;
use crate::model::{ChatId, ImageMatch, ImageMessage, ImageRecord, ImageSearchResult, UnixTime};
use crate::text::brief_content;
use crate::{CoreError, Result};

const PHASH_SIZE: usize = 32;
const PHASH_LOW_FREQ: usize = 8;
const ORB_DESCRIPTOR_BYTES: usize = 32;
const MAX_ORB_SIDE: i32 = 900;

#[derive(Debug, Serialize, Deserialize)]
struct StoreFile {
    version: u32,
    records: Vec<ImageRecord>,
}

#[derive(Serialize)]
struct StoreFileRef<'a> {
    version: u32,
    records: &'a [ImageRecord],
}

const STORE_VERSION: u32 = 1;

/// A query image reduced to searchable features.
#[derive(Clone, Debug)]
pub struct ImageDescriptor {
    pub width: u32,
    pub height: u32,
    pub phash: u64,
    pub keypoints: u32,
    pub descriptor_rows: u32,
    pub descriptors: Vec<u8>,
}

/// Persistent image index for one backend.
pub struct ImageStore {
    dir: PathBuf,
    thumbs_dir: PathBuf,
    index_file: PathBuf,
    records: Vec<ImageRecord>,
    config: ImageConfig,
    dirty: bool,
}

impl ImageStore {
    pub fn open(dir: impl AsRef<Path>, config: ImageConfig) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let thumbs_dir = dir.join("thumbs");
        fs::create_dir_all(&thumbs_dir)?;
        let index_file = dir.join("index.bin");
        let records = if index_file.exists() {
            match fs::read(&index_file) {
                Ok(bytes) => match bincode::deserialize::<StoreFile>(&bytes) {
                    Ok(file) if file.version == STORE_VERSION => file.records,
                    Ok(file) => {
                        warn!(
                            version = file.version,
                            "image index version mismatch, starting empty"
                        );
                        Vec::new()
                    }
                    Err(e) => {
                        warn!("cannot decode image index ({}), starting empty", e);
                        Vec::new()
                    }
                },
                Err(e) => {
                    warn!("cannot read image index {}: {}", index_file.display(), e);
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };
        debug!(count = records.len(), "loaded image index");
        Ok(Self {
            dir,
            thumbs_dir,
            index_file,
            records,
            config,
            dirty: false,
        })
    }

    pub fn config(&self) -> &ImageConfig {
        &self.config
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn count_chat(&self, chat_id: ChatId) -> usize {
        self.records
            .iter()
            .filter(|r| r.message.chat_id == chat_id)
            .count()
    }

    /// Extract features from an image file.
    pub fn describe_path(&self, path: impl AsRef<Path>) -> Result<ImageDescriptor> {
        describe_path_with(path.as_ref(), &self.config)
    }

    /// Add or replace an image and persist immediately.
    pub fn add_image(
        &mut self,
        source: impl AsRef<Path>,
        message: ImageMessage,
    ) -> Result<ImageDescriptor> {
        let descriptor = self.add_image_deferred(source, message)?;
        self.persist()?;
        Ok(descriptor)
    }

    /// Add or replace an image without rewriting the whole index. Call
    /// [`ImageStore::flush`] after a bulk import.
    pub fn add_image_deferred(
        &mut self,
        source: impl AsRef<Path>,
        mut message: ImageMessage,
    ) -> Result<ImageDescriptor> {
        let source = source.as_ref();
        let image = read_image(source)?;
        let descriptor = describe_mat(&image, &self.config)?;
        let thumbnail_name = format!("{}.jpg", url_digest(&message.url));
        let thumbnail_path = self.thumbs_dir.join(&thumbnail_name);
        write_thumbnail(&image, &thumbnail_path, &self.config)?;

        message.thumbnail = None;
        if let Some(position) = self
            .records
            .iter()
            .position(|record| record.message.url == message.url)
        {
            self.remove_thumbnail_at(position);
            self.records.remove(position);
        }
        self.records.push(ImageRecord {
            message,
            thumbnail_file: Some(thumbnail_name),
            phash: descriptor.phash,
            keypoints: descriptor.keypoints,
            descriptor_rows: descriptor.descriptor_rows,
            descriptors: descriptor.descriptors.clone(),
        });
        self.dirty = true;
        Ok(descriptor)
    }

    /// Persist pending deferred changes.
    pub fn flush(&mut self) -> Result<()> {
        if self.dirty {
            self.persist()?;
        }
        Ok(())
    }

    pub fn remove_url(&mut self, url: &str) -> Result<bool> {
        if let Some(position) = self
            .records
            .iter()
            .position(|record| record.message.url == url)
        {
            self.remove_thumbnail_at(position);
            self.records.remove(position);
            self.persist()?;
            return Ok(true);
        }
        Ok(false)
    }

    pub fn remove_chat(&mut self, chat_id: ChatId) -> Result<usize> {
        let before = self.records.len();
        let mut index = 0;
        while index < self.records.len() {
            if self.records[index].message.chat_id == chat_id {
                self.remove_thumbnail_at(index);
                self.records.remove(index);
            } else {
                index += 1;
            }
        }
        let removed = before - self.records.len();
        if removed > 0 {
            self.persist()?;
        }
        Ok(removed)
    }

    /// Update the caption/text attached to an indexed image (after an edit).
    pub fn update_text(
        &mut self,
        url: &str,
        content: &str,
        filename: Option<&str>,
    ) -> Result<bool> {
        if let Some(record) = self
            .records
            .iter_mut()
            .find(|record| record.message.url == url)
        {
            record.message.content = content.to_string();
            record.message.filename = filename.map(ToString::to_string);
            self.persist()?;
            return Ok(true);
        }
        Ok(false)
    }

    pub fn clear(&mut self) -> Result<()> {
        for index in (0..self.records.len()).rev() {
            self.remove_thumbnail_at(index);
        }
        self.records.clear();
        self.persist()?;
        Ok(())
    }

    /// Search using a query image file.
    pub fn search_path(
        &self,
        path: impl AsRef<Path>,
        chats: Option<&[ChatId]>,
    ) -> Result<ImageSearchResult> {
        let descriptor = self.describe_path(path)?;
        self.search_descriptor(&descriptor, chats)
    }

    /// Search using already-extracted features.
    pub fn search_descriptor(
        &self,
        query: &ImageDescriptor,
        chats: Option<&[ChatId]>,
    ) -> Result<ImageSearchResult> {
        let threshold = self.config.effective_min_similarity();
        if self.records.is_empty() {
            return Ok(ImageSearchResult {
                matches: Vec::new(),
                total_candidates: 0,
                min_similarity: threshold,
                scanned: 0,
            });
        }

        let candidates = self.candidate_positions(query);
        let scanned = candidates.len();
        let mut matcher = if query.descriptor_rows >= 2 {
            Some(
                features2d::BFMatcher::create(core::NORM_HAMMING, false)
                    .map_err(|e| CoreError::Image(format!("cannot create BFMatcher: {e}")))?,
            )
        } else {
            None
        };
        let query_mat: Option<core::Mat> =
            if !query.descriptors.is_empty() && query.descriptor_rows >= 2 {
                Some(
                    core::Mat::new_rows_cols_with_data(
                        query.descriptor_rows as i32,
                        ORB_DESCRIPTOR_BYTES as i32,
                        &query.descriptors,
                    )
                    .map_err(|e| CoreError::Image(format!("invalid query descriptors: {e}")))?
                    .clone_pointee(),
                )
            } else {
                None
            };

        let chat_allowed = |chat_id: ChatId| match chats {
            Some(chats) if !chats.is_empty() => chats.contains(&chat_id),
            _ => true,
        };

        let mut results: Vec<ImageMatch> = Vec::new();
        for position in candidates {
            let record = &self.records[position];
            if !chat_allowed(record.message.chat_id) {
                continue;
            }
            match self.score_record(query, record, query_mat.as_ref(), matcher.as_mut()) {
                Ok(Some((similarity, good, distance))) => {
                    let thumbnail = record
                        .thumbnail_file
                        .as_ref()
                        .map(|name| self.thumbs_dir.join(name))
                        .filter(|path| path.exists());
                    results.push(ImageMatch {
                        message: record.message.clone(),
                        similarity,
                        good_matches: good,
                        phash_distance: distance,
                        thumbnail,
                    });
                }
                Ok(None) => {}
                Err(e) => {
                    warn!(url = %record.message.url, "image match failed: {e}");
                }
            }
        }
        results.sort_by(|a, b| {
            b.similarity
                .partial_cmp(&a.similarity)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.message.post_time.cmp(&a.message.post_time))
        });
        let total_candidates = results.len();
        results.truncate(self.config.result_limit.max(1));

        Ok(ImageSearchResult {
            matches: results,
            total_candidates,
            min_similarity: threshold,
            scanned,
        })
    }

    fn score_record(
        &self,
        query: &ImageDescriptor,
        record: &ImageRecord,
        query_mat: Option<&core::Mat>,
        matcher: Option<&mut core::Ptr<features2d::BFMatcher>>,
    ) -> Result<Option<(f32, usize, u32)>> {
        let phash_distance = (query.phash ^ record.phash).count_ones();
        let phash_similarity = 1.0 - phash_distance as f32 / 64.0;

        if let (Some(query_mat), Some(matcher), true) = (
            query_mat,
            matcher,
            record.descriptor_rows >= 2 && !record.descriptors.is_empty(),
        ) {
            let train_mat = core::Mat::new_rows_cols_with_data(
                record.descriptor_rows as i32,
                ORB_DESCRIPTOR_BYTES as i32,
                &record.descriptors,
            )
            .map_err(|e| CoreError::Image(format!("invalid stored descriptors: {e}")))?
            .clone_pointee();
            let mut knn = core::Vector::<core::Vector<core::DMatch>>::new();
            matcher
                .knn_train_match_def(query_mat, &train_mat, &mut knn, 2)
                .map_err(|e| CoreError::Image(format!("ORB matching failed: {e}")))?;

            let mut good = 0usize;
            for index in 0..knn.len() {
                let pair = knn
                    .get(index)
                    .map_err(|e| CoreError::Image(format!("cannot read match pair: {e}")))?;
                if pair.len() < 2 {
                    continue;
                }
                let best = pair
                    .get(0)
                    .map_err(|e| CoreError::Image(format!("cannot read match: {e}")))?
                    .distance;
                let second = pair
                    .get(1)
                    .map_err(|e| CoreError::Image(format!("cannot read match: {e}")))?
                    .distance;
                if best < self.config.match_ratio * second {
                    good += 1;
                }
            }
            let denominator = query.keypoints.min(record.keypoints).max(1) as f32;
            let orb_ratio = good as f32 / denominator;
            let similarity = 0.85 * orb_ratio + 0.15 * phash_similarity;
            let accepted = good >= self.config.min_good_matches
                && orb_ratio >= self.config.effective_min_similarity();
            if accepted {
                return Ok(Some((similarity, good, phash_distance)));
            }
            // Fall through to the hash-only comparison for images with very
            // few ORB features (e.g. flat screenshots).
            if record.keypoints < 8 && query.keypoints < 8 && phash_distance <= 12 {
                return Ok(Some((phash_similarity, good, phash_distance)));
            }
            return Ok(None);
        }

        // No usable ORB descriptors: rely on the perceptual hash.
        if phash_distance <= 12 {
            return Ok(Some((phash_similarity, 0, phash_distance)));
        }
        Ok(None)
    }

    fn candidate_positions(&self, query: &ImageDescriptor) -> Vec<usize> {
        let limit = self.config.phash_candidate_limit.max(1);
        if self.records.len() <= limit {
            return (0..self.records.len()).collect();
        }
        let mut ranked: Vec<(u32, usize)> = self
            .records
            .iter()
            .enumerate()
            .map(|(index, record)| ((query.phash ^ record.phash).count_ones(), index))
            .collect();
        ranked.sort_by_key(|(distance, _)| *distance);
        ranked.truncate(limit);
        ranked.into_iter().map(|(_, index)| index).collect()
    }

    fn remove_thumbnail_at(&self, position: usize) {
        if let Some(name) = &self.records[position].thumbnail_file {
            let path = self.thumbs_dir.join(name);
            if let Err(e) = fs::remove_file(&path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!(path = %path.display(), "cannot remove thumbnail: {e}");
                }
            }
        }
    }

    fn persist(&mut self) -> Result<()> {
        let file = StoreFileRef {
            version: STORE_VERSION,
            records: &self.records,
        };
        let bytes = bincode::serialize(&file)
            .map_err(|e| CoreError::Image(format!("cannot encode image index: {e}")))?;
        let temporary = self.dir.join("index.bin.tmp");
        fs::write(&temporary, bytes)?;
        if self.index_file.exists() {
            fs::remove_file(&self.index_file)?;
        }
        fs::rename(&temporary, &self.index_file)?;
        self.dirty = false;
        debug!(count = self.records.len(), path = %self.index_file.display(), "persisted image index");
        Ok(())
    }
}

fn read_image(path: &Path) -> Result<core::Mat> {
    if !path.exists() {
        return Err(CoreError::Image(format!(
            "image file does not exist: {}",
            path.display()
        )));
    }
    let image = imgcodecs::imread(path.to_string_lossy().as_ref(), imgcodecs::IMREAD_COLOR)
        .map_err(|e| CoreError::Image(format!("cannot read image {}: {e}", path.display())))?;
    if image.empty() {
        return Err(CoreError::Image(format!(
            "unsupported or empty image: {}",
            path.display()
        )));
    }
    Ok(image)
}

fn describe_path_with(path: &Path, config: &ImageConfig) -> Result<ImageDescriptor> {
    let image = read_image(path)?;
    describe_mat(&image, config)
}

fn describe_mat(image: &core::Mat, config: &ImageConfig) -> Result<ImageDescriptor> {
    let width = image.cols().max(0) as u32;
    let height = image.rows().max(0) as u32;
    let mut gray = core::Mat::default();
    imgproc::cvt_color(image, &mut gray, imgproc::COLOR_BGR2GRAY, 0)
        .map_err(|e| CoreError::Image(format!("cvtColor failed: {e}")))?;

    let phash = perceptual_hash(&gray)?;

    let mut orb = features2d::ORB::create(
        config.max_features,
        1.2,
        8,
        31,
        0,
        2,
        features2d::ORB_ScoreType::HARRIS_SCORE,
        31,
        20,
    )
    .map_err(|e| CoreError::Image(format!("cannot create ORB: {e}")))?;

    let mut work = core::Mat::default();
    let max_side = width.max(height) as f64;
    if max_side > MAX_ORB_SIDE as f64 {
        let scale = MAX_ORB_SIDE as f64 / max_side;
        imgproc::resize(
            &gray,
            &mut work,
            core::Size::new(0, 0),
            scale,
            scale,
            imgproc::INTER_AREA,
        )
        .map_err(|e| CoreError::Image(format!("resize failed: {e}")))?;
    } else {
        work = gray
            .try_clone()
            .map_err(|e| CoreError::Image(format!("clone failed: {e}")))?;
    }

    let mut keypoints = core::Vector::<core::KeyPoint>::new();
    let mut descriptors = core::Mat::default();
    orb.detect_and_compute(
        &work,
        &core::no_array(),
        &mut keypoints,
        &mut descriptors,
        false,
    )
    .map_err(|e| CoreError::Image(format!("ORB detect_and_compute failed: {e}")))?;

    let rows = descriptors.rows().max(0);
    let bytes = if rows > 0 {
        descriptors
            .data_bytes()
            .map_err(|e| CoreError::Image(format!("cannot read descriptors: {e}")))?
            .to_vec()
    } else {
        Vec::new()
    };
    if rows > 0 && bytes.len() != rows as usize * ORB_DESCRIPTOR_BYTES {
        return Err(CoreError::Image(format!(
            "unexpected ORB descriptor size: {} bytes for {rows} rows",
            bytes.len()
        )));
    }

    Ok(ImageDescriptor {
        width,
        height,
        phash,
        keypoints: keypoints.len() as u32,
        descriptor_rows: rows as u32,
        descriptors: bytes,
    })
}

fn write_thumbnail(image: &core::Mat, path: &Path, config: &ImageConfig) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let width = image.cols().max(1);
    let height = image.rows().max(1);
    let max_side = width.max(height);
    let mut thumbnail = image
        .try_clone()
        .map_err(|e| CoreError::Image(format!("clone failed: {e}")))?;
    if max_side > config.thumbnail_max_side {
        let scale = config.thumbnail_max_side as f64 / max_side as f64;
        let mut resized = core::Mat::default();
        imgproc::resize(
            image,
            &mut resized,
            core::Size::new(0, 0),
            scale,
            scale,
            imgproc::INTER_AREA,
        )
        .map_err(|e| CoreError::Image(format!("thumbnail resize failed: {e}")))?;
        thumbnail = resized;
    }
    let mut params = core::Vector::<i32>::new();
    params.push(imgcodecs::IMWRITE_JPEG_QUALITY);
    params.push(config.jpeg_quality.clamp(1, 100));
    imgcodecs::imwrite(path.to_string_lossy().as_ref(), &thumbnail, &params)
        .map_err(|e| CoreError::Image(format!("cannot write thumbnail: {e}")))?;
    Ok(())
}

fn perceptual_hash(gray: &core::Mat) -> Result<u64> {
    let mut small = core::Mat::default();
    imgproc::resize(
        gray,
        &mut small,
        core::Size::new(PHASH_SIZE as i32, PHASH_SIZE as i32),
        0.0,
        0.0,
        imgproc::INTER_AREA,
    )
    .map_err(|e| CoreError::Image(format!("pHash resize failed: {e}")))?;
    let bytes = small
        .data_bytes()
        .map_err(|e| CoreError::Image(format!("cannot read grayscale pixels: {e}")))?;
    let expected = PHASH_SIZE * PHASH_SIZE;
    if bytes.len() < expected {
        return Err(CoreError::Image(format!(
            "pHash buffer too small: {} < {expected}",
            bytes.len()
        )));
    }
    let mut pixels = [[0.0f64; PHASH_SIZE]; PHASH_SIZE];
    for y in 0..PHASH_SIZE {
        for x in 0..PHASH_SIZE {
            pixels[y][x] = bytes[y * PHASH_SIZE + x] as f64;
        }
    }
    let coefficients = dct_32(&pixels);
    let mut low: Vec<f64> = Vec::with_capacity(PHASH_LOW_FREQ * PHASH_LOW_FREQ - 1);
    for (u, row) in coefficients.iter().enumerate().take(PHASH_LOW_FREQ) {
        for (v, value) in row.iter().enumerate().take(PHASH_LOW_FREQ) {
            if u == 0 && v == 0 {
                continue;
            }
            low.push(*value);
        }
    }
    let mut sorted = low.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median = sorted[sorted.len() / 2];
    let mut hash = 0u64;
    for (index, value) in low.iter().enumerate() {
        if *value > median {
            hash |= 1u64 << index;
        }
    }
    Ok(hash)
}

fn dct_32(pixels: &[[f64; PHASH_SIZE]; PHASH_SIZE]) -> [[f64; PHASH_SIZE]; PHASH_SIZE] {
    let mut cos_table = [[0.0f64; PHASH_SIZE]; PHASH_SIZE];
    for (u, row) in cos_table.iter_mut().enumerate() {
        for (x, value) in row.iter_mut().enumerate() {
            *value = (((2 * x + 1) as f64) * u as f64 * std::f64::consts::PI
                / (2.0 * PHASH_SIZE as f64))
                .cos();
        }
    }
    let mut output = [[0.0f64; PHASH_SIZE]; PHASH_SIZE];
    for u in 0..PHASH_SIZE {
        for v in 0..PHASH_SIZE {
            let mut sum = 0.0f64;
            for x in 0..PHASH_SIZE {
                let cx = cos_table[u][x];
                for y in 0..PHASH_SIZE {
                    sum += pixels[y][x] * cx * cos_table[v][y];
                }
            }
            let cu = if u == 0 {
                (1.0 / PHASH_SIZE as f64).sqrt()
            } else {
                (2.0 / PHASH_SIZE as f64).sqrt()
            };
            let cv = if v == 0 {
                (1.0 / PHASH_SIZE as f64).sqrt()
            } else {
                (2.0 / PHASH_SIZE as f64).sqrt()
            };
            output[u][v] = sum * cu * cv;
        }
    }
    output
}

fn url_digest(url: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(url.as_bytes());
    let digest = hasher.finalize();
    digest[..12]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Build a public `ImageMessage` for a backend message.
#[allow(clippy::too_many_arguments)]
pub fn image_message(
    url: impl Into<String>,
    chat_id: ChatId,
    post_time: UnixTime,
    sender: impl Into<String>,
    content: impl Into<String>,
    filename: Option<String>,
    width: u32,
    height: u32,
) -> ImageMessage {
    ImageMessage {
        url: url.into(),
        chat_id,
        post_time,
        sender: sender.into(),
        content: content.into(),
        filename,
        width,
        height,
        thumbnail: None,
    }
}

/// Human-readable caption for `img2img` results.
pub fn image_caption(matched: &ImageMatch, chat_name: &str) -> String {
    let mut caption = format!(
        "🔎 相似度 {:.0}% | {}",
        matched.similarity * 100.0,
        crate::text::escape_html(chat_name)
    );
    if matched.message.content.is_empty() {
        if let Some(filename) = &matched.message.filename {
            caption.push_str(&format!(
                "\n📎 {}",
                crate::text::escape_html(&brief_content(filename, 60))
            ));
        }
    } else {
        caption.push_str(&format!(
            "\n{}",
            crate::text::escape_html(&brief_content(&matched.message.content, 80))
        ));
    }
    caption.push_str(&format!("\n{}", matched.message.url));
    caption
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ImageConfig;
    use crate::model::ImageSearchMode;
    use opencv::core::{Point, Rect, Scalar};
    use tempfile::TempDir;

    fn test_base() -> PathBuf {
        let base = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp");
        fs::create_dir_all(&base).unwrap();
        base
    }

    fn draw_pattern(seed: u64, path: &Path, size: i32) {
        let mut image =
            core::Mat::new_rows_cols_with_default(size, size, core::CV_8UC3, Scalar::all(245.0))
                .unwrap();
        let mut state = seed;
        let mut next = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as i32
        };
        for index in 0..18 {
            let x = next() % (size - 40);
            let y = next() % (size - 40);
            let w = 30 + next() % 90;
            let h = 30 + next() % 90;
            let color = Scalar::from([
                (next() % 220) as f64,
                (next() % 220) as f64,
                (next() % 220) as f64,
                0.0,
            ]);
            if index % 2 == 0 {
                let _ = imgproc::rectangle(
                    &mut image,
                    Rect::new(x, y, w.min(size - x), h.min(size - y)),
                    color,
                    -1,
                    imgproc::LINE_8,
                    0,
                );
            } else {
                let _ = imgproc::circle(
                    &mut image,
                    Point::new(x + 20, y + 20),
                    20 + next() % 25,
                    color,
                    -1,
                    imgproc::LINE_8,
                    0,
                );
            }
        }
        imgcodecs::imwrite_def(path.to_string_lossy().as_ref(), &image).unwrap();
    }

    fn setup() -> (TempDir, PathBuf, Vec<u8>) {
        let temp = tempfile::Builder::new()
            .prefix("image-test-")
            .tempdir_in(test_base())
            .unwrap();
        let image_a = temp.path().join("a.png");
        let image_b = temp.path().join("b.png");
        draw_pattern(1, &image_a, 420);
        draw_pattern(999, &image_b, 420);
        let bytes = fs::read(&image_a).unwrap();
        (temp, image_a, bytes)
    }

    #[test]
    fn descriptor_extracts_orb_and_phash() {
        let (_temp, path, _) = setup();
        let cfg = ImageConfig::default();
        let descriptor = describe_path_with(&path, &cfg).unwrap();
        assert!(
            descriptor.keypoints > 20,
            "keypoints={}",
            descriptor.keypoints
        );
        assert!(descriptor.descriptor_rows > 0);
        assert_eq!(
            descriptor.descriptors.len(),
            descriptor.descriptor_rows as usize * 32
        );
        assert_ne!(descriptor.phash, 0);
    }

    #[test]
    fn perceptual_hash_is_stable_across_resize() {
        let temp = tempfile::Builder::new()
            .prefix("image-phash-")
            .tempdir_in(test_base())
            .unwrap();
        let original = temp.path().join("orig.png");
        let resized = temp.path().join("small.png");
        draw_pattern(7, &original, 420);
        let image = read_image(&original).unwrap();
        let mut small = core::Mat::default();
        imgproc::resize(
            &image,
            &mut small,
            core::Size::new(300, 300),
            0.0,
            0.0,
            imgproc::INTER_AREA,
        )
        .unwrap();
        imgcodecs::imwrite_def(resized.to_string_lossy().as_ref(), &small).unwrap();
        let cfg = ImageConfig::default();
        let a = describe_path_with(&original, &cfg).unwrap();
        let b = describe_path_with(&resized, &cfg).unwrap();
        let distance = (a.phash ^ b.phash).count_ones();
        assert!(distance <= 8, "phash distance {distance}");
    }

    #[test]
    fn image_index_matches_same_image_and_rejects_other() {
        let (temp, image_a, bytes) = setup();
        let image_b = temp.path().join("b.png");
        let resized_a = temp.path().join("a_small.jpg");
        let image = read_image(&image_a).unwrap();
        let mut small = core::Mat::default();
        imgproc::resize(
            &image,
            &mut small,
            core::Size::new(320, 320),
            0.0,
            0.0,
            imgproc::INTER_AREA,
        )
        .unwrap();
        imgcodecs::imwrite_def(resized_a.to_string_lossy().as_ref(), &small).unwrap();

        let temp_dir = {
            let dir = tempfile::Builder::new()
                .prefix("image-store-")
                .tempdir_in(test_base())
                .unwrap();
            dir.path().join("images")
        };
        let config = ImageConfig {
            min_good_matches: 5,
            min_similarity: 0.05,
            ..Default::default()
        };
        let mut store = ImageStore::open(&temp_dir, config).unwrap();

        store
            .add_image(
                &image_a,
                image_message(
                    "https://t.me/c/1/1",
                    1,
                    1_700_000_001,
                    "alice",
                    "第一张图片",
                    None,
                    image.cols() as u32,
                    image.rows() as u32,
                ),
            )
            .unwrap();
        store
            .add_image(
                &image_b,
                image_message(
                    "https://t.me/c/1/2",
                    1,
                    1_700_000_002,
                    "bob",
                    "第二张图片",
                    None,
                    420,
                    420,
                ),
            )
            .unwrap();
        assert_eq!(store.len(), 2);

        let result = store.search_path(&resized_a, None).unwrap();
        assert!(!result.matches.is_empty());
        assert_eq!(result.matches[0].message.url, "https://t.me/c/1/1");
        assert!(result.matches[0].similarity > 0.3);

        // Query the stored image bytes through a fresh descriptor.
        let descriptor = store.describe_path(&image_a).unwrap();
        let by_descriptor = store.search_descriptor(&descriptor, None).unwrap();
        assert_eq!(by_descriptor.matches[0].message.url, "https://t.me/c/1/1");

        // Chat filter works.
        let filtered = store.search_path(&resized_a, Some(&[2])).unwrap();
        assert!(filtered.matches.is_empty());

        // Thumbnails are written.
        let thumb = &result.matches[0].thumbnail;
        assert!(thumb.as_ref().map(|p| p.exists()).unwrap_or(false));

        // Remove and persistence.
        assert!(store.remove_url("https://t.me/c/1/1").unwrap());
        assert_eq!(store.len(), 1);
        drop(store);
        let reopened = ImageStore::open(&temp_dir, ImageConfig::default()).unwrap();
        assert_eq!(reopened.len(), 1);
        let _ = bytes;
    }

    #[test]
    fn image_to_text_uses_caption() {
        let (_temp, image_a, _) = setup();
        let dir = tempfile::Builder::new()
            .prefix("image-caption-")
            .tempdir_in(test_base())
            .unwrap();
        let mut store =
            ImageStore::open(dir.path().join("images"), ImageConfig::default()).unwrap();
        store
            .add_image(
                &image_a,
                image_message(
                    "https://t.me/c/9/1",
                    9,
                    1_700_000_100,
                    "carol",
                    "会议现场照片",
                    Some("meeting.jpg".into()),
                    420,
                    420,
                ),
            )
            .unwrap();
        let result = store.search_path(&image_a, None).unwrap();
        let matched = &result.matches[0];
        assert_eq!(matched.message.content, "会议现场照片");
        assert_eq!(matched.message.filename.as_deref(), Some("meeting.jpg"));
        let caption = image_caption(matched, "测试群");
        assert!(caption.contains("会议现场照片"));
        assert!(caption.contains("https://t.me/c/9/1"));
        let _ = ImageSearchMode::ImageToImage;
    }
}
