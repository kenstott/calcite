/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.govdata.sec;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Semantic text chunker for SEC filings and other structured documents.
 *
 * <p>Creates optimal text chunks for downstream processing (embeddings, RAG, NLP) by:
 * <ul>
 *   <li>Tracking heading hierarchy and flushing pending chunks at section boundaries</li>
 *   <li>Respecting natural document boundaries (paragraphs, tables, lists)</li>
 *   <li>Combining small adjacent elements to reach target chunk size</li>
 *   <li>Splitting oversized atomic content with [TABLE/LIST:start/cont/end] markers</li>
 *   <li>Splitting oversized prose at sentence boundaries with paragraph continuation flags</li>
 *   <li>Recording the section path (LCA breadcrumb) on every chunk</li>
 * </ul>
 */
@SuppressWarnings("UnusedVariable")
public class SemanticTextChunker {
  private static final Logger LOGGER = LoggerFactory.getLogger(SemanticTextChunker.class);

  public static final int DEFAULT_TARGET_SIZE = 1650;
  public static final int DEFAULT_MIN_SIZE = 1100;
  public static final int DEFAULT_MAX_SIZE = 2200;

  // Hard-split threshold: maxSize * this factor (matches Chonk's max×1.15)
  private static final double HARD_SPLIT_FACTOR = 1.15;

  private static final Pattern SENTENCE_BOUNDARY = Pattern.compile(
      "(?<=[.!?])\\s+(?=[A-Z])|(?<=[.!?])\\s*$");

  private static final Pattern TOC_PATTERN = Pattern.compile(
      "(?i)^\\s*(item\\s+\\d+|part\\s+[iv]+|table of contents).*\\d+\\s*$");

  private static final Pattern PAGE_PATTERN = Pattern.compile(
      "(?i)^\\s*(page\\s*)?\\d+\\s*$|^\\s*-\\s*\\d+\\s*-\\s*$");

  // SEC filing heading patterns for styled (non-h1-h6) elements
  private static final Pattern PART_HEADING_PATTERN = Pattern.compile(
      "(?i)^part\\s+[ivxIVX]+[.:]?(?:\\s+.*)?$");
  private static final Pattern ITEM_HEADING_PATTERN = Pattern.compile(
      "(?i)^item\\s+\\d+[a-zA-Z]?[.:]?(?:\\s+.*)?$");

  private final int targetSize;
  private final int minSize;
  private final int maxSize;

  public enum ContentType {
    PARAGRAPH,
    TABLE,
    LIST,
    HEADING,
    FOOTNOTE,
    BLOCKQUOTE,
    MIXED
  }

  // ── Heading stack entry ───────────────────────────────────────────────────

  private static class HeadingEntry {
    final int level;
    final String text;

    HeadingEntry(int level, String text) {
      this.level = level;
      this.text = text;
    }
  }

  // ── Chunk record ─────────────────────────────────────────────────────────

  public static class Chunk {
    private final String text;
    private final ContentType contentType;
    private final int sequenceNumber;
    private final String sourceElements;
    private final List<String> footnoteRefs;
    private final List<String> sectionPath;
    private final boolean paragraphContinuation;

    public Chunk(String text, ContentType contentType, int sequenceNumber,
                 String sourceElements, List<String> footnoteRefs,
                 List<String> sectionPath, boolean paragraphContinuation) {
      this.text = text;
      this.contentType = contentType;
      this.sequenceNumber = sequenceNumber;
      this.sourceElements = sourceElements;
      this.footnoteRefs = footnoteRefs != null ? footnoteRefs : new ArrayList<>();
      this.sectionPath = sectionPath != null ? sectionPath : new ArrayList<>();
      this.paragraphContinuation = paragraphContinuation;
    }

    // Backwards-compatible constructor — no section path, not a continuation
    public Chunk(String text, ContentType contentType, int sequenceNumber,
                 String sourceElements, List<String> footnoteRefs) {
      this(text, contentType, sequenceNumber, sourceElements, footnoteRefs,
           new ArrayList<>(), false);
    }

    public String getText() { return text; }
    public ContentType getContentType() { return contentType; }
    public int getSequenceNumber() { return sequenceNumber; }
    public String getSourceElements() { return sourceElements; }
    public List<String> getFootnoteRefs() { return footnoteRefs; }
    public List<String> getSectionPath() { return sectionPath; }
    public boolean isParagraphContinuation() { return paragraphContinuation; }
    public int getLength() { return text != null ? text.length() : 0; }
  }

  // ── ElementContent ────────────────────────────────────────────────────────

  private static class ElementContent {
    final String text;
    final ContentType type;
    final String tagName;
    final boolean isAtomic;
    final List<String> footnoteRefs;
    final int headingLevel; // 0 for non-headings, 1-6 for hN

    ElementContent(String text, ContentType type, String tagName, boolean isAtomic,
                   List<String> footnoteRefs, int headingLevel) {
      this.text = text;
      this.type = type;
      this.tagName = tagName;
      this.isAtomic = isAtomic;
      this.footnoteRefs = footnoteRefs;
      this.headingLevel = headingLevel;
    }
  }

  // ── Constructors ──────────────────────────────────────────────────────────

  public SemanticTextChunker() {
    this(DEFAULT_TARGET_SIZE, DEFAULT_MIN_SIZE, DEFAULT_MAX_SIZE);
  }

  public SemanticTextChunker(int targetSize, int minSize, int maxSize) {
    this.targetSize = targetSize;
    this.minSize = minSize;
    this.maxSize = maxSize;
  }

  // ── Public API ────────────────────────────────────────────────────────────

  public List<Chunk> chunkFromElement(Element startElement, String stopPattern) {
    if (startElement == null) {
      return new ArrayList<>();
    }
    Pattern stopRegex = stopPattern != null
        ? Pattern.compile(stopPattern, Pattern.CASE_INSENSITIVE) : null;
    List<ElementContent> contents = extractElementContents(startElement, stopRegex);
    return combineIntoChunks(contents);
  }

  public List<Chunk> chunkPlainText(String text) {
    return chunkPlainText(text, new ArrayList<>());
  }

  public List<Chunk> chunkSection(Document doc, String cssSelector, String stopPattern) {
    Elements elements = doc.select(cssSelector);
    if (elements.isEmpty()) {
      return new ArrayList<>();
    }
    return chunkFromElement(elements.first(), stopPattern);
  }

  // ── Element extraction ────────────────────────────────────────────────────

  private List<ElementContent> extractElementContents(Element startElement, Pattern stopPattern) {
    List<ElementContent> contents = new ArrayList<>();
    Element current = startElement;

    while (current != null) {
      String text = current.text().trim();

      if (stopPattern != null && stopPattern.matcher(text).find()) {
        break;
      }
      if (isNoise(text)) {
        current = current.nextElementSibling();
        continue;
      }

      ElementContent content = extractContent(current);
      if (content != null && !content.text.isEmpty()) {
        contents.add(content);
      }
      current = current.nextElementSibling();
    }

    return contents;
  }

  private ElementContent extractContent(Element element) {
    String tagName = element.tagName().toLowerCase();
    String text;
    ContentType type;
    boolean isAtomic = false;
    List<String> footnoteRefs;
    int headingLevel = 0;

    switch (tagName) {
      case "table":
        text = extractTableText(element);
        type = ContentType.TABLE;
        isAtomic = true;
        footnoteRefs = extractFootnoteReferences(text);
        break;

      case "ul":
      case "ol":
        text = extractListText(element);
        type = ContentType.LIST;
        isAtomic = true;
        footnoteRefs = extractFootnoteReferences(text);
        break;

      case "h1":
      case "h2":
      case "h3":
      case "h4":
      case "h5":
      case "h6":
        text = cleanText(element.text());
        type = ContentType.HEADING;
        footnoteRefs = new ArrayList<>();
        headingLevel = Integer.parseInt(tagName.substring(1));
        break;

      case "blockquote":
        text = cleanText(element.text());
        type = ContentType.BLOCKQUOTE;
        footnoteRefs = extractFootnoteReferences(text);
        break;

      default:
        text = cleanText(element.text());
        type = ContentType.PARAGRAPH;
        footnoteRefs = extractFootnoteReferences(text);
        // Detect styled headings common in SEC filings (bold spans, ITEM/PART patterns)
        headingLevel = inferStyledHeadingLevel(element, text);
        if (headingLevel > 0) {
          type = ContentType.HEADING;
          footnoteRefs = new ArrayList<>();
        }
        break;
    }

    if (text == null || text.isEmpty()) {
      return null;
    }

    return new ElementContent(text, type, tagName, isAtomic, footnoteRefs, headingLevel);
  }

  // ── Core chunking algorithm ───────────────────────────────────────────────

  /**
   * Combines extracted content elements into optimal chunks using the Chonk algorithm:
   * headings update the section stack and trigger a flush when the pending chunk
   * has reached min_size; oversized atomic content is split with continuation markers;
   * oversized prose is split at sentence boundaries with paragraphContinuation flags.
   */
  private List<Chunk> combineIntoChunks(List<ElementContent> contents) {
    List<Chunk> chunks = new ArrayList<>();
    StringBuilder currentChunk = new StringBuilder();
    List<String> currentTags = new ArrayList<>();
    List<String> currentFootnotes = new ArrayList<>();
    ContentType currentType = null;
    int chunkNum = 1;

    // Heading stack: (level, text) entries — parity with Chonk heading_stack
    List<HeadingEntry> headingStack = new ArrayList<>();
    // Section path snapshot at the start of the current pending chunk
    List<String> chunkSectionPath = new ArrayList<>();

    for (ElementContent content : contents) {

      // ── Heading: update stack, optionally flush, then skip to next element ──
      if (content.type == ContentType.HEADING) {
        // Pop entries at the same or deeper level
        while (!headingStack.isEmpty()
            && headingStack.get(headingStack.size() - 1).level >= content.headingLevel) {
          headingStack.remove(headingStack.size() - 1);
        }
        headingStack.add(new HeadingEntry(content.headingLevel, content.text));

        // Flush pending chunk when it has reached min_size (Chonk rule)
        if (currentChunk.length() >= minSize) {
          chunks.add(createChunk(currentChunk.toString(), currentType, chunkNum++,
              currentTags, currentFootnotes, chunkSectionPath, false));
          currentChunk = new StringBuilder();
          currentTags = new ArrayList<>();
          currentFootnotes = new ArrayList<>();
          currentType = null;
        }

        // Update section path snapshot for any content that follows
        chunkSectionPath = currentSectionPath(headingStack);
        continue;
      }

      // ── Atomic content (table / list) ─────────────────────────────────────
      if (content.isAtomic) {
        if (currentChunk.length() > 0) {
          chunks.add(createChunk(currentChunk.toString(), currentType, chunkNum++,
              currentTags, currentFootnotes, chunkSectionPath, false));
          currentChunk = new StringBuilder();
          currentTags = new ArrayList<>();
          currentFootnotes = new ArrayList<>();
          currentType = null;
        }

        int hardLimit = (int) (maxSize * HARD_SPLIT_FACTOR);
        if (content.text.length() > hardLimit) {
          // Split oversized atomic content with continuation markers
          List<String> pieces = content.type == ContentType.TABLE
              ? splitAtRows(content.text)
              : splitAtItems(content.text);
          String markerType = content.type == ContentType.TABLE ? "TABLE" : "LIST";
          for (int i = 0; i < pieces.size(); i++) {
            String marker = i == 0 ? "[" + markerType + ":start]"
                : (i == pieces.size() - 1 ? "[" + markerType + ":end]"
                : "[" + markerType + ":cont]");
            String pieceText = marker + "\n" + pieces.get(i);
            chunks.add(new Chunk(pieceText, content.type, chunkNum++,
                content.tagName, content.footnoteRefs,
                new ArrayList<>(chunkSectionPath), i > 0));
          }
        } else {
          chunks.add(new Chunk(content.text, content.type, chunkNum++,
              content.tagName, content.footnoteRefs,
              new ArrayList<>(chunkSectionPath), false));
        }
        continue;
      }

      // ── Oversized prose: split at sentence boundaries ─────────────────────
      if (content.text.length() > (int) (maxSize * HARD_SPLIT_FACTOR)) {
        if (currentChunk.length() > 0) {
          chunks.add(createChunk(currentChunk.toString(), currentType, chunkNum++,
              currentTags, currentFootnotes, chunkSectionPath, false));
          currentChunk = new StringBuilder();
          currentTags = new ArrayList<>();
          currentFootnotes = new ArrayList<>();
          currentType = null;
        }
        List<Chunk> splitChunks = chunkPlainText(content.text, new ArrayList<>(chunkSectionPath));
        for (Chunk splitChunk : splitChunks) {
          chunks.add(new Chunk(splitChunk.getText(), content.type, chunkNum++,
              content.tagName, splitChunk.getFootnoteRefs(),
              splitChunk.getSectionPath(), splitChunk.isParagraphContinuation()));
        }
        continue;
      }

      // ── Normal prose accumulation ─────────────────────────────────────────

      // Flush if adding this content would exceed maxSize and we already have minSize
      if (currentChunk.length() + content.text.length() > maxSize
          && currentChunk.length() >= minSize) {
        chunks.add(createChunk(currentChunk.toString(), currentType, chunkNum++,
            currentTags, currentFootnotes, chunkSectionPath, false));
        currentChunk = new StringBuilder();
        currentTags = new ArrayList<>();
        currentFootnotes = new ArrayList<>();
        currentType = null;
      }

      if (currentChunk.length() == 0) {
        // Snapshot the section path when this chunk starts
        chunkSectionPath = new ArrayList<>(chunkSectionPath);
      }

      if (currentChunk.length() > 0) {
        currentChunk.append("\n\n");
      }
      currentChunk.append(content.text);
      currentTags.add(content.tagName);
      currentFootnotes.addAll(content.footnoteRefs);

      if (currentType == null) {
        currentType = content.type;
      } else if (currentType != content.type) {
        currentType = ContentType.MIXED;
      }
    }

    // Emit final pending chunk
    if (currentChunk.length() > 0) {
      chunks.add(createChunk(currentChunk.toString(), currentType, chunkNum,
          currentTags, currentFootnotes, chunkSectionPath, false));
    }

    return chunks;
  }

  // ── Plain text chunking ───────────────────────────────────────────────────

  private List<Chunk> chunkPlainText(String text, List<String> sectionPath) {
    List<Chunk> chunks = new ArrayList<>();
    if (text == null || text.trim().isEmpty()) {
      return chunks;
    }

    String cleanText = cleanText(text);
    if (cleanText.length() <= maxSize) {
      chunks.add(new Chunk(cleanText, ContentType.PARAGRAPH, 1, "text",
          extractFootnoteReferences(cleanText), sectionPath, false));
      return chunks;
    }

    List<String> sentences = splitIntoSentences(cleanText);
    StringBuilder current = new StringBuilder();
    int chunkNum = 1;
    boolean isFirst = true;

    for (String sentence : sentences) {
      if (current.length() + sentence.length() > targetSize && current.length() >= minSize) {
        String chunkText = current.toString().trim();
        if (!chunkText.isEmpty()) {
          chunks.add(new Chunk(chunkText, ContentType.PARAGRAPH, chunkNum++, "text",
              extractFootnoteReferences(chunkText), sectionPath, !isFirst));
          isFirst = false;
        }
        current = new StringBuilder();
      }
      current.append(sentence).append(" ");
    }

    String remaining = current.toString().trim();
    if (!remaining.isEmpty()) {
      chunks.add(new Chunk(remaining, ContentType.PARAGRAPH, chunkNum, "text",
          extractFootnoteReferences(remaining), sectionPath, !isFirst));
    }

    return chunks;
  }

  // ── Atomic content splitting ──────────────────────────────────────────────

  private List<String> splitAtRows(String tableText) {
    return splitLinesIntoPieces(tableText);
  }

  private List<String> splitAtItems(String listText) {
    return splitLinesIntoPieces(listText);
  }

  private List<String> splitLinesIntoPieces(String text) {
    List<String> pieces = new ArrayList<>();
    String[] lines = text.split("\n");
    StringBuilder piece = new StringBuilder();

    for (String line : lines) {
      if (piece.length() + line.length() + 1 > maxSize && piece.length() > 0) {
        pieces.add(piece.toString().trim());
        piece = new StringBuilder();
      }
      if (piece.length() > 0) {
        piece.append("\n");
      }
      piece.append(line);
    }
    if (piece.length() > 0) {
      pieces.add(piece.toString().trim());
    }
    if (pieces.isEmpty()) {
      pieces.add(text);
    }
    return pieces;
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private List<String> currentSectionPath(List<HeadingEntry> stack) {
    List<String> path = new ArrayList<>();
    for (HeadingEntry entry : stack) {
      path.add(entry.text);
    }
    return path;
  }

  private Chunk createChunk(String text, ContentType type, int seqNum,
                             List<String> tags, List<String> footnotes,
                             List<String> sectionPath, boolean continuation) {
    String tagStr = String.join(",", tags);
    return new Chunk(text.trim(),
        type != null ? type : ContentType.PARAGRAPH,
        seqNum, tagStr, footnotes,
        new ArrayList<>(sectionPath), continuation);
  }

  private String extractTableText(Element table) {
    StringBuilder sb = new StringBuilder();
    Elements rows = table.select("tr");

    for (Element row : rows) {
      Elements cells = row.select("th, td");
      List<String> cellTexts = new ArrayList<>();
      for (Element cell : cells) {
        String cellText = cleanText(cell.text());
        if (!cellText.isEmpty()) {
          cellTexts.add(cellText);
        }
      }
      if (!cellTexts.isEmpty()) {
        sb.append(String.join(" | ", cellTexts)).append("\n");
      }
    }

    return sb.toString().trim();
  }

  private String extractListText(Element list) {
    StringBuilder sb = new StringBuilder();
    Elements items = list.select("li");
    int itemNum = 1;

    for (Element item : items) {
      String itemText = cleanText(item.text());
      if (!itemText.isEmpty()) {
        if (list.tagName().equals("ol")) {
          sb.append(itemNum++).append(". ");
        } else {
          sb.append("• ");
        }
        sb.append(itemText).append("\n");
      }
    }

    return sb.toString().trim();
  }

  private List<String> splitIntoSentences(String text) {
    List<String> sentences = new ArrayList<>();
    Matcher matcher = SENTENCE_BOUNDARY.matcher(text);
    int lastEnd = 0;

    while (matcher.find()) {
      int endPos = Math.min(matcher.start() + 1, text.length());
      String sentence = text.substring(lastEnd, endPos).trim();
      if (!sentence.isEmpty()) {
        sentences.add(sentence);
      }
      lastEnd = matcher.end();
    }

    if (lastEnd < text.length()) {
      String remaining = text.substring(lastEnd).trim();
      if (!remaining.isEmpty()) {
        sentences.add(remaining);
      }
    }

    if (sentences.isEmpty() && !text.trim().isEmpty()) {
      sentences.add(text.trim());
    }

    return sentences;
  }

  private String cleanText(String text) {
    if (text == null) {
      return "";
    }
    text = text.replaceAll("\\s+", " ").trim();
    text = text.replaceAll("&[a-zA-Z]+;", " ");
    text = text.replaceAll("&#\\d+;", " ");
    text = text.replaceAll("\\s+", " ").trim();
    return text;
  }

  private boolean isNoise(String text) {
    if (text == null || text.isEmpty()) {
      return true;
    }
    return TOC_PATTERN.matcher(text).matches() || PAGE_PATTERN.matcher(text).matches();
  }

  private static int inferStyledHeadingLevel(Element element, String text) {
    if (text == null || text.isEmpty() || text.length() > 200) {
      return 0;
    }
    if (PART_HEADING_PATTERN.matcher(text).matches()) {
      return 1;
    }
    if (ITEM_HEADING_PATTERN.matcher(text).matches()) {
      return 2;
    }
    if (text.length() <= 150 && isEntirelyBold(element, text)) {
      return 3;
    }
    return 0;
  }

  private static boolean isEntirelyBold(Element element, String fullText) {
    if (isBoldStyle(element.attr("style"))) {
      return true;
    }
    Elements boldSpans = element.select(
        "span[style*=font-weight:700], span[style*=font-weight: 700],"
        + " span[style*=font-weight:bold], b, strong");
    if (boldSpans.isEmpty()) {
      return false;
    }
    StringBuilder boldText = new StringBuilder();
    for (Element b : boldSpans) {
      boldText.append(b.text().trim()).append(" ");
    }
    return boldText.toString().trim().length() >= fullText.length() * 0.80;
  }

  private static boolean isBoldStyle(String style) {
    if (style == null || style.isEmpty()) {
      return false;
    }
    String s = style.replaceAll("\\s+", "").toLowerCase();
    return s.contains("font-weight:700") || s.contains("font-weight:800")
        || s.contains("font-weight:900") || s.contains("font-weight:bold");
  }

  private List<String> extractFootnoteReferences(String text) {
    List<String> refs = new ArrayList<>();
    if (text == null) {
      return refs;
    }
    Pattern footnotePattern = Pattern.compile("\\(\\d+\\)|\\[\\d+\\]|[*†‡§¶]|\\(\\*\\)");
    Matcher matcher = footnotePattern.matcher(text);
    while (matcher.find()) {
      String ref = matcher.group().trim();
      if (!refs.contains(ref)) {
        refs.add(ref);
      }
    }
    return refs;
  }

  // ── Factory methods ───────────────────────────────────────────────────────

  public static SemanticTextChunker forMDA() {
    return new SemanticTextChunker(1650, 1100, 2200);
  }

  public static SemanticTextChunker forFootnotes() {
    return new SemanticTextChunker(1000, 400, 1800);
  }

  public static SemanticTextChunker forEarnings() {
    return new SemanticTextChunker(1800, 1100, 2500);
  }

  public static SemanticTextChunker forRiskFactors() {
    return new SemanticTextChunker(1650, 1100, 2200);
  }
}
