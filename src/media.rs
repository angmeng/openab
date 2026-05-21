use crate::acp::ContentBlock;
use crate::config::SttConfig;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use image::ImageReader;
use std::io::Cursor;
use std::sync::LazyLock;
use tracing::{debug, error};

/// Reusable HTTP client for downloading attachments (shared across adapters).
pub static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("static HTTP client must build")
});

/// Maximum dimension (width or height) for resized images.
const IMAGE_MAX_DIMENSION_PX: u32 = 1200;

/// JPEG quality for compressed output.
const IMAGE_JPEG_QUALITY: u8 = 75;

/// Download an image from a URL, resize/compress it, and return as a ContentBlock.
/// Pass `auth_token` for platforms that require authentication (e.g. Slack private files).
pub async fn download_and_encode_image(
    url: &str,
    mime_hint: Option<&str>,
    filename: &str,
    size: u64,
    auth_token: Option<&str>,
) -> Option<ContentBlock> {
    const MAX_SIZE: u64 = 10 * 1024 * 1024; // 10 MB

    if url.is_empty() {
        return None;
    }

    let mime = mime_hint.or_else(|| {
        filename
            .rsplit('.')
            .next()
            .and_then(|ext| match ext.to_lowercase().as_str() {
                "png" => Some("image/png"),
                "jpg" | "jpeg" => Some("image/jpeg"),
                "gif" => Some("image/gif"),
                "webp" => Some("image/webp"),
                _ => None,
            })
    });

    let Some(mime) = mime else {
        debug!(filename, "skipping non-image attachment");
        return None;
    };
    let mime = mime.split(';').next().unwrap_or(mime).trim();
    if !mime.starts_with("image/") {
        debug!(filename, mime, "skipping non-image attachment");
        return None;
    }

    if size > MAX_SIZE {
        error!(filename, size, "image exceeds 10MB limit");
        return None;
    }

    let mut req = HTTP_CLIENT.get(url);
    if let Some(token) = auth_token {
        req = req.header("Authorization", format!("Bearer {token}"));
    }

    let response = match req.send().await {
        Ok(resp) => resp,
        Err(e) => {
            error!(url, error = %e, "download failed");
            return None;
        }
    };
    if !response.status().is_success() {
        error!(url, status = %response.status(), "HTTP error downloading image");
        return None;
    }
    let bytes = match response.bytes().await {
        Ok(b) => b,
        Err(e) => {
            error!(url, error = %e, "read failed");
            return None;
        }
    };

    if bytes.len() as u64 > MAX_SIZE {
        error!(
            filename,
            size = bytes.len(),
            "downloaded image exceeds limit"
        );
        return None;
    }

    let (output_bytes, output_mime) = match resize_and_compress(&bytes) {
        Ok(result) => result,
        Err(e) => {
            if bytes.len() > 1024 * 1024 {
                error!(filename, error = %e, size = bytes.len(), "resize failed and original too large, skipping");
                return None;
            }
            debug!(filename, error = %e, "resize failed, using original");
            (bytes.to_vec(), mime.to_string())
        }
    };

    debug!(
        filename,
        original_size = bytes.len(),
        compressed_size = output_bytes.len(),
        "image processed"
    );

    let encoded = BASE64.encode(&output_bytes);
    Some(ContentBlock::Image {
        media_type: output_mime,
        data: encoded,
    })
}

/// Download an audio file and transcribe it via the configured STT provider.
/// Pass `auth_token` for platforms that require authentication.
pub async fn download_and_transcribe(
    url: &str,
    filename: &str,
    mime_type: &str,
    size: u64,
    stt_config: &SttConfig,
    auth_token: Option<&str>,
) -> Option<String> {
    const MAX_SIZE: u64 = 25 * 1024 * 1024; // 25 MB (Whisper API limit)

    if size > MAX_SIZE {
        error!(filename, size, "audio exceeds 25MB limit");
        return None;
    }

    let mut req = HTTP_CLIENT.get(url);
    if let Some(token) = auth_token {
        req = req.header("Authorization", format!("Bearer {token}"));
    }

    let resp = req.send().await.ok()?;
    if !resp.status().is_success() {
        error!(url, status = %resp.status(), "audio download failed");
        return None;
    }
    let bytes = resp.bytes().await.ok()?.to_vec();

    crate::stt::transcribe(
        &HTTP_CLIENT,
        stt_config,
        bytes,
        filename.to_string(),
        mime_type,
    )
    .await
}

/// Resize image so longest side <= IMAGE_MAX_DIMENSION_PX, then encode as JPEG.
/// GIFs are passed through unchanged to preserve animation.
pub fn resize_and_compress(raw: &[u8]) -> Result<(Vec<u8>, String), image::ImageError> {
    let reader = ImageReader::new(Cursor::new(raw)).with_guessed_format()?;

    let format = reader.format();

    if format == Some(image::ImageFormat::Gif) {
        return Ok((raw.to_vec(), "image/gif".to_string()));
    }

    let img = reader.decode()?;
    let (w, h) = (img.width(), img.height());

    let img = if w > IMAGE_MAX_DIMENSION_PX || h > IMAGE_MAX_DIMENSION_PX {
        let max_side = std::cmp::max(w, h);
        let ratio = f64::from(IMAGE_MAX_DIMENSION_PX) / f64::from(max_side);
        let new_w = (f64::from(w) * ratio) as u32;
        let new_h = (f64::from(h) * ratio) as u32;
        img.resize(new_w, new_h, image::imageops::FilterType::Lanczos3)
    } else {
        img
    };

    let mut buf = Cursor::new(Vec::new());
    let encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut buf, IMAGE_JPEG_QUALITY);
    img.write_with_encoder(encoder)?;

    Ok((buf.into_inner(), "image/jpeg".to_string()))
}

/// Check if a MIME type is audio.
pub fn is_audio_mime(mime: &str) -> bool {
    mime.starts_with("audio/")
}

/// Check if an attachment is a video file.
pub fn is_video_file(filename: &str, content_type: Option<&str>) -> bool {
    let mime = content_type.unwrap_or("");
    let mime_base = mime.split(';').next().unwrap_or(mime).trim();
    if mime_base.starts_with("video/") {
        return true;
    }

    filename
        .rsplit('.')
        .next()
        .map(|ext| {
            matches!(
                ext.to_lowercase().as_str(),
                "mp4" | "mov" | "m4v" | "webm" | "mkv" | "avi"
            )
        })
        .unwrap_or(false)
}

/// Extensions recognised as text-based files that can be inlined into the prompt.
const TEXT_EXTENSIONS: &[&str] = &[
    "txt", "csv", "log", "md", "json", "jsonl", "yaml", "yml", "toml", "xml", "rs", "py", "js",
    "ts", "jsx", "tsx", "go", "java", "c", "cpp", "h", "hpp", "rb", "sh", "bash", "zsh", "fish",
    "ps1", "bat", "sql", "html", "css", "scss", "less", "ini", "cfg", "conf", "env",
];

/// Exact filenames (no extension) recognised as text files.
const TEXT_FILENAMES: &[&str] = &[
    "dockerfile",
    "makefile",
    "justfile",
    "rakefile",
    "gemfile",
    "procfile",
    "vagrantfile",
    ".gitignore",
    ".dockerignore",
    ".editorconfig",
];

/// MIME types recognised as text-based (beyond `text/*`).
const TEXT_MIME_TYPES: &[&str] = &[
    "application/json",
    "application/xml",
    "application/javascript",
    "application/x-yaml",
    "application/x-sh",
    "application/toml",
    "application/x-toml",
];

/// Check if a file is text-based and can be inlined into the prompt.
pub fn is_text_file(filename: &str, content_type: Option<&str>) -> bool {
    let mime = content_type.unwrap_or("");
    let mime_base = mime.split(';').next().unwrap_or(mime).trim();
    if mime_base.starts_with("text/") || TEXT_MIME_TYPES.contains(&mime_base) {
        return true;
    }
    // Check extension
    if filename.contains('.') {
        if let Some(ext) = filename.rsplit('.').next() {
            if TEXT_EXTENSIONS.contains(&ext.to_lowercase().as_str()) {
                return true;
            }
        }
    }
    // Check exact filename (Dockerfile, Makefile, etc.)
    TEXT_FILENAMES.contains(&filename.to_lowercase().as_str())
}

/// Download a text-based file and return it as a ContentBlock::Text.
/// Files larger than 512 KB are skipped to avoid bloating the prompt.
///
/// Pass `auth_token` for platforms that require authentication (e.g. Slack private files).
///
/// Note: the caller already guards total size via a total cap; the per-file
/// MAX_SIZE check here is intentional defense-in-depth so this function remains
/// self-contained and safe when called from other contexts.
pub async fn download_and_read_text_file(
    url: &str,
    filename: &str,
    size: u64,
    auth_token: Option<&str>,
) -> Option<(ContentBlock, u64)> {
    const MAX_SIZE: u64 = 512 * 1024; // 512 KB

    if size > MAX_SIZE {
        tracing::warn!(filename, size, "text file exceeds 512KB limit, skipping");
        return None;
    }

    let mut req = HTTP_CLIENT.get(url);
    if let Some(token) = auth_token {
        req = req.header("Authorization", format!("Bearer {token}"));
    }

    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(url, error = %e, "text file download failed");
            return None;
        }
    };
    if !resp.status().is_success() {
        tracing::warn!(url, status = %resp.status(), "text file download failed");
        return None;
    }
    let bytes = resp.bytes().await.ok()?;
    let actual_size = bytes.len() as u64;

    // Defense-in-depth: verify actual download size
    if actual_size > MAX_SIZE {
        tracing::warn!(
            filename,
            size = actual_size,
            "downloaded text file exceeds 512KB limit, skipping"
        );
        return None;
    }

    // from_utf8_lossy returns Cow::Borrowed for valid UTF-8 (zero-copy)
    let text = String::from_utf8_lossy(&bytes).into_owned();

    // Dynamic fence: keep adding backticks until the fence doesn't appear in content
    let mut fence = "```".to_string();
    while text.contains(fence.as_str()) {
        fence.push('`');
    }

    debug!(filename, bytes = text.len(), "text file inlined");
    Some((
        ContentBlock::Text {
            text: format!("[File: {filename}]\n{fence}\n{text}\n{fence}"),
        },
        actual_size,
    ))
}


/// Sanitize a single path component so it cannot escape its parent dir or
/// hit reserved characters on common filesystems.
fn sanitize_path_component(s: &str) -> String {
    let cleaned: String = s
        .chars()
        .map(|c| match c {
            '/' | '\\' | '\0' | ':' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();
    let trimmed = cleaned.trim_matches(|c: char| c == '.' || c.is_whitespace());
    if trimmed.is_empty() {
        "file".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Default per-file size cap when `OPENAB_ATTACHMENTS_MAX_MB` is unset.
const ATTACHMENT_DEFAULT_MAX_MB: u64 = 200;

/// Default attachments directory when `OPENAB_ATTACHMENTS_DIR` is unset.
const ATTACHMENT_DEFAULT_DIR: &str = "/tmp/openab-attachments";

/// Resolve the max attachment size in bytes, honoring `OPENAB_ATTACHMENTS_MAX_MB`.
fn attachments_max_bytes() -> u64 {
    std::env::var("OPENAB_ATTACHMENTS_MAX_MB")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(ATTACHMENT_DEFAULT_MAX_MB)
        * 1024
        * 1024
}

/// Resolve the configured attachments base directory.
fn attachments_base_dir() -> std::path::PathBuf {
    std::env::var("OPENAB_ATTACHMENTS_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from(ATTACHMENT_DEFAULT_DIR))
}

/// Download an arbitrary attachment to disk and return a ContentBlock::Text
/// referencing the on-disk path. This is the fallback for files that are
/// neither audio (handled by STT), nor inlineable text, nor images.
///
/// `bucket_id` is a stable per-message id (Slack `ts`, Discord message id).
/// Files end up at `${OPENAB_ATTACHMENTS_DIR:-/tmp/openab-attachments}/<bucket>/<filename>`.
///
/// Size cap: `OPENAB_ATTACHMENTS_MAX_MB` (default 200 MB).
///
/// Path containment: after building the target path, we canonicalize both the
/// base dir and the target dir and verify the target stays under base. This
/// guards against pathological filenames slipping past `sanitize_path_component`
/// (e.g. via symlinks introduced into the base dir out-of-band).
///
/// Returns None on download/write failure, oversized files, or containment violation.
pub async fn download_to_disk(
    url: &str,
    filename: &str,
    mime: &str,
    size: u64,
    auth_token: Option<&str>,
    bucket_id: &str,
) -> Option<ContentBlock> {
    let max_size = attachments_max_bytes();

    if url.is_empty() {
        return None;
    }
    if size > 0 && size > max_size {
        tracing::warn!(filename, size, max = max_size, "attachment exceeds size limit, skipping");
        return None;
    }

    let base = attachments_base_dir();
    let safe_bucket = sanitize_path_component(bucket_id);
    let safe_name = sanitize_path_component(filename);
    let target_dir = base.join(&safe_bucket);

    if let Err(e) = tokio::fs::create_dir_all(&target_dir).await {
        tracing::error!(?target_dir, error = %e, "failed to create attachment dir");
        return None;
    }

    // Path containment check: after the dir exists, canonicalize and verify
    // the target_dir is still under base. Symlinks or `..` in env-provided
    // base resolve here. We canonicalize base too so both sides are absolute.
    let canonical_base = match tokio::fs::canonicalize(&base).await {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(?base, error = %e, "failed to canonicalize attachments base dir");
            return None;
        }
    };
    let canonical_target_dir = match tokio::fs::canonicalize(&target_dir).await {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(?target_dir, error = %e, "failed to canonicalize target dir");
            return None;
        }
    };
    if !canonical_target_dir.starts_with(&canonical_base) {
        tracing::error!(
            ?canonical_base,
            ?canonical_target_dir,
            "path containment violation, refusing to write"
        );
        return None;
    }
    let target_path = canonical_target_dir.join(&safe_name);

    let mut req = HTTP_CLIENT.get(url);
    if let Some(token) = auth_token {
        req = req.header("Authorization", format!("Bearer {token}"));
    }
    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(url, error = %e, "attachment download failed");
            return None;
        }
    };
    if !resp.status().is_success() {
        tracing::error!(url, status = %resp.status(), "attachment download failed");
        return None;
    }
    let bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            tracing::error!(url, error = %e, "attachment read failed");
            return None;
        }
    };
    if bytes.len() as u64 > max_size {
        tracing::error!(filename, size = bytes.len(), max = max_size, "downloaded attachment exceeds size limit");
        return None;
    }

    if let Err(e) = tokio::fs::write(&target_path, &bytes).await {
        tracing::error!(?target_path, error = %e, "failed to write attachment");
        return None;
    }

    let mime_label = if mime.is_empty() {
        "application/octet-stream"
    } else {
        mime
    };
    debug!(
        filename = %safe_name,
        path = %target_path.display(),
        size = bytes.len(),
        mime = mime_label,
        "attachment saved to disk",
    );

    Some(ContentBlock::Text {
        text: format!(
            "[Attachment received]\nFilename: {}\nType: {}\nSize: {} bytes\nSaved to: {}\nUse the appropriate skill to read or process this file.",
            safe_name,
            mime_label,
            bytes.len(),
            target_path.display(),
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_png(width: u32, height: u32) -> Vec<u8> {
        let img = image::RgbImage::new(width, height);
        let mut buf = Cursor::new(Vec::new());
        img.write_to(&mut buf, image::ImageFormat::Png).unwrap();
        buf.into_inner()
    }

    #[test]
    fn large_image_resized_to_max_dimension() {
        let png = make_png(3000, 2000);
        let (compressed, mime) = resize_and_compress(&png).unwrap();

        assert_eq!(mime, "image/jpeg");
        let result = image::load_from_memory(&compressed).unwrap();
        assert!(result.width() <= IMAGE_MAX_DIMENSION_PX);
        assert!(result.height() <= IMAGE_MAX_DIMENSION_PX);
    }

    #[test]
    fn small_image_keeps_original_dimensions() {
        let png = make_png(800, 600);
        let (compressed, mime) = resize_and_compress(&png).unwrap();

        assert_eq!(mime, "image/jpeg");
        let result = image::load_from_memory(&compressed).unwrap();
        assert_eq!(result.width(), 800);
        assert_eq!(result.height(), 600);
    }

    #[test]
    fn landscape_image_respects_aspect_ratio() {
        let png = make_png(4000, 2000);
        let (compressed, _) = resize_and_compress(&png).unwrap();

        let result = image::load_from_memory(&compressed).unwrap();
        assert_eq!(result.width(), 1200);
        assert_eq!(result.height(), 600);
    }

    #[test]
    fn portrait_image_respects_aspect_ratio() {
        let png = make_png(2000, 4000);
        let (compressed, _) = resize_and_compress(&png).unwrap();

        let result = image::load_from_memory(&compressed).unwrap();
        assert_eq!(result.width(), 600);
        assert_eq!(result.height(), 1200);
    }

    #[test]
    fn compressed_output_is_smaller_than_original() {
        let png = make_png(3000, 2000);
        let (compressed, _) = resize_and_compress(&png).unwrap();

        assert!(
            compressed.len() < png.len(),
            "compressed {} should be < original {}",
            compressed.len(),
            png.len()
        );
    }

    #[test]
    fn gif_passes_through_unchanged() {
        let gif: Vec<u8> = vec![
            0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x2C,
            0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x02, 0x02, 0x44, 0x01, 0x00,
            0x3B,
        ];
        let (output, mime) = resize_and_compress(&gif).unwrap();

        assert_eq!(mime, "image/gif");
        assert_eq!(output, gif);
    }

    #[test]
    fn invalid_data_returns_error() {
        let garbage = vec![0x00, 0x01, 0x02, 0x03];
        assert!(resize_and_compress(&garbage).is_err());
    }

    #[test]
    fn video_file_detects_mime_and_common_extensions() {
        assert!(is_video_file("clip.bin", Some("video/mp4")));
        assert!(is_video_file("clip.mp4", None));
        assert!(is_video_file("clip.MOV", None));
        assert!(!is_video_file("notes.txt", Some("text/plain")));
    }

    #[test]
    fn sanitize_strips_path_separators_and_control_chars() {
        // `/` and `\` become `_`. Leading `..` is trimmed (dot-trim), so
        // `../etc/passwd` -> `.._etc_passwd` -> `_etc_passwd`. The leading
        // underscore is acceptable: the value is joined under a known base
        // dir, never used as an absolute path.
        assert_eq!(sanitize_path_component("../etc/passwd"), "_etc_passwd");
        assert_eq!(sanitize_path_component("foo\\bar"), "foo_bar");
        assert_eq!(sanitize_path_component("a\0b"), "a_b");
        assert_eq!(sanitize_path_component("C:report.xlsx"), "C_report.xlsx");
        assert_eq!(sanitize_path_component(".....hidden"), "hidden");
        assert_eq!(sanitize_path_component(""), "file");
        assert_eq!(sanitize_path_component("..."), "file");
        // Control chars (newline, tab, etc.) → underscore.
        assert_eq!(sanitize_path_component("a\nb\tc"), "a_b_c");
    }

    #[test]
    fn attachments_max_bytes_honors_env_override() {
        // SAFETY: tests share env; use a scoped guard pattern.
        let original = std::env::var("OPENAB_ATTACHMENTS_MAX_MB").ok();
        std::env::set_var("OPENAB_ATTACHMENTS_MAX_MB", "50");
        assert_eq!(attachments_max_bytes(), 50 * 1024 * 1024);
        std::env::remove_var("OPENAB_ATTACHMENTS_MAX_MB");
        assert_eq!(attachments_max_bytes(), 200 * 1024 * 1024);
        if let Some(v) = original {
            std::env::set_var("OPENAB_ATTACHMENTS_MAX_MB", v);
        }
    }

}
