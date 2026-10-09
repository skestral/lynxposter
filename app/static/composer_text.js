(function () {
  const segmenter = typeof Intl.Segmenter === "function"
    ? new Intl.Segmenter(undefined, {granularity: "grapheme"})
    : null;

  window.lynxposterComposerText = {
    length(text, unit) {
      if (unit !== "graphemes") return text.length;
      // Older browsers defer grapheme validation to the server.
      return segmenter ? Array.from(segmenter.segment(text)).length : null;
    },
    utf8Bytes(text) {
      return new TextEncoder().encode(text).length;
    },
  };
})();
