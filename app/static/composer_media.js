(function () {
  function fileSize(bytes) {
    if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
    const units = ["B", "KB", "MB", "GB"];
    let value = bytes;
    let unitIndex = 0;
    while (value >= 1000 && unitIndex < units.length - 1) {
      value /= 1000;
      unitIndex += 1;
    }
    return `${Number(value.toFixed(unitIndex === 0 ? 0 : 2))} ${units[unitIndex]}`;
  }

  window.lynxposterComposerMedia = {
    fileSize,
    limitSummary(rule) {
      const limits = [];
      if (rule.prepared_image_max_bytes != null) limits.push(`${fileSize(rule.prepared_image_max_bytes)} per prepared image`);
      if (rule.prepared_video_max_bytes != null) limits.push(`${fileSize(rule.prepared_video_max_bytes)} per prepared video`);
      if (rule.video_max_duration_seconds != null) {
        const seconds = rule.video_max_duration_seconds;
        limits.push(seconds % 60 === 0 ? `${seconds / 60} min video maximum` : `${seconds} sec video maximum`);
      }
      return limits;
    },
    preparationNotices(rule, snapshot) {
      const notices = [];
      const largeImages = (snapshot.imageSizes || []).filter((size) => rule.prepared_image_max_bytes != null && size > rule.prepared_image_max_bytes).length;
      const largeVideos = (snapshot.videoSizes || []).filter((size) => rule.prepared_video_max_bytes != null && size > rule.prepared_video_max_bytes).length;
      if (largeImages) notices.push(`${largeImages} original image${largeImages === 1 ? " exceeds" : "s exceed"} ${fileSize(rule.prepared_image_max_bytes)}. Publishing will resize/compress images to fit; originals are preserved.`);
      if (largeVideos) notices.push(`${largeVideos} original video${largeVideos === 1 ? " exceeds" : "s exceed"} ${fileSize(rule.prepared_video_max_bytes)}. Publishing will transcode videos to fit; originals are preserved. Videos over the duration limit must be shortened before publishing.`);
      return notices;
    },
  };
})();
