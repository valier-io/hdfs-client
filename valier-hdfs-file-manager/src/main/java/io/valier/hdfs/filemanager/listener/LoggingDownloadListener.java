package io.valier.hdfs.filemanager.listener;

import io.valier.hdfs.filemanager.DownloadRequest;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Builder;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * A configurable implementation of DownloadListener that logs download lifecycle events to SLF4J.
 *
 * <p>This listener provides configurable logging levels and throttled progress updates to avoid
 * overwhelming the logs with frequent onDataDownloaded events. Progress updates are only logged at
 * a configurable cadence (e.g., every 3 seconds) to provide useful information without excessive
 * verbosity.
 *
 * <p>Uses System.nanoTime() for precise timing measurements and TimeUnit for all time conversions.
 * Since DownloadRequest is immutable, the entire request object is used as the map key for tracking
 * progress timing per download.
 *
 * <p>Example usage:
 *
 * <pre>
 * // Use default settings (INFO level, 3-second intervals)
 * LoggingDownloadListener listener = LoggingDownloadListener.defaultInstance();
 *
 * // Or customize the settings
 * LoggingDownloadListener customListener = LoggingDownloadListener.builder()
 *     .loggingLevel(Level.DEBUG)
 *     .progressUpdateIntervalSeconds(5)
 *     .build();
 *
 * DownloadRequest request = DownloadRequest.builder()
 *     .hdfsFilePath("/large-file.txt")
 *     .localFilePath(Paths.get("/local/file.txt"))
 *     .downloadListener(listener)
 *     .build();
 * </pre>
 */
@Value
@Builder
public class LoggingDownloadListener implements DownloadListener {

  private static final Logger log = LoggerFactory.getLogger(LoggingDownloadListener.class);

  /**
   * The logging level to use for all download lifecycle events. Defaults to INFO level if not
   * specified.
   */
  @Builder.Default Level loggingLevel = Level.INFO;

  /**
   * Interval in seconds between progress update log messages. Progress updates via onDataDownloaded
   * will only be logged at this cadence to avoid overwhelming the logs. Defaults to 3 seconds if
   * not specified.
   */
  @Builder.Default int progressUpdateIntervalSeconds = 3;

  /**
   * Tracks the last time a progress update was logged for each download request. Uses the entire
   * DownloadRequest object as the key since it's immutable and provides proper equals/hashCode
   * semantics for tracking timing per download.
   */
  Map<DownloadRequest, Long> lastProgressLogTime = new ConcurrentHashMap<>();

  /**
   * Returns a default instance of LoggingDownloadListener with INFO level logging and 3-second
   * progress update intervals.
   *
   * @return a default LoggingDownloadListener instance
   */
  public static LoggingDownloadListener defaultInstance() {
    return LoggingDownloadListener.builder().build();
  }

  @Override
  public void onDownloadStarted(DownloadRequest request) {
    // Initialize progress tracking for this request with current nano time
    lastProgressLogTime.put(request, System.nanoTime());

    logAtLevel("Download started: {} -> {}", request.getHdfsFilePath(), request.getLocalFilePath());
  }

  @Override
  public void onDataDownloaded(DownloadRequest request, int bytesWritten, long totalBytesWritten) {
    long currentTimeNanos = System.nanoTime();

    // Get the last time we logged progress for this request
    Long lastLogTimeNanos = lastProgressLogTime.get(request);
    if (lastLogTimeNanos == null) {
      lastLogTimeNanos = currentTimeNanos;
      lastProgressLogTime.put(request, currentTimeNanos);
    }

    // Check if enough time has passed since the last progress log
    long timeSinceLastLogNanos = currentTimeNanos - lastLogTimeNanos;
    long intervalNanos = TimeUnit.SECONDS.toNanos(progressUpdateIntervalSeconds);

    if (timeSinceLastLogNanos >= intervalNanos) {
      logAtLevel(
          "Download progress: {} - {} bytes written ({} bytes total)",
          request.getHdfsFilePath(),
          formatBytes(bytesWritten),
          formatBytes(totalBytesWritten));

      // Update the last log time
      lastProgressLogTime.put(request, currentTimeNanos);
    }
  }

  @Override
  public void onDownloadCompleted(
      DownloadRequest request, long fileSizeBytes, long downloadTimeMs) {
    // Clean up progress tracking for this request
    lastProgressLogTime.remove(request);

    // Convert download time from milliseconds to seconds for throughput calculation using TimeUnit
    double downloadTimeSeconds =
        TimeUnit.MILLISECONDS.toNanos(downloadTimeMs) / (double) TimeUnit.SECONDS.toNanos(1);
    double throughputMBps =
        fileSizeBytes > 0 && downloadTimeSeconds > 0
            ? (fileSizeBytes / 1024.0 / 1024.0) / downloadTimeSeconds
            : 0.0;

    logAtLevel(
        "Download completed: {} - {} in {}ms ({:.2f} MB/s)",
        request.getHdfsFilePath(),
        formatBytes(fileSizeBytes),
        downloadTimeMs,
        throughputMBps);
  }

  @Override
  public void onDownloadFailed(DownloadRequest request, Exception exception) {
    // Clean up progress tracking for this request
    lastProgressLogTime.remove(request);

    logAtLevel(
        "Download failed: {} - {}: {}",
        request.getHdfsFilePath(),
        exception.getClass().getSimpleName(),
        exception.getMessage());
  }

  /**
   * Logs a message at the configured logging level using SLF4J.
   *
   * @param message the message format string
   * @param args the arguments for the message format
   */
  private void logAtLevel(String message, Object... args) {
    switch (loggingLevel) {
      case TRACE:
        log.trace(message, args);
        break;
      case DEBUG:
        log.debug(message, args);
        break;
      case INFO:
        log.info(message, args);
        break;
      case WARN:
        log.warn(message, args);
        break;
      case ERROR:
        log.error(message, args);
        break;
      default:
        log.info(message, args);
        break;
    }
  }

  /**
   * Formats byte counts into human-readable strings with appropriate units.
   *
   * @param bytes the number of bytes to format
   * @return formatted string like "1.5 KB", "2.3 MB", etc.
   */
  private String formatBytes(long bytes) {
    if (bytes < 1024) {
      return bytes + " B";
    } else if (bytes < 1024 * 1024) {
      return String.format("%.1f KB", bytes / 1024.0);
    } else if (bytes < 1024 * 1024 * 1024) {
      return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
    } else {
      return String.format("%.1f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }
  }
}
