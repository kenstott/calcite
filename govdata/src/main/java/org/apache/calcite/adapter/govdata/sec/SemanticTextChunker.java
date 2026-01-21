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
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Semantic text chunker for SEC filings and other structured documents.
 *
 * <p>Creates optimal text chunks for downstream processing (embeddings, RAG, NLP) by:
 * <ul>
 *   <li>Respecting natural document boundaries (paragraphs, tables, lists)</li>
 *   <li>Combining small adjacent elements to reach target chunk size</li>
 *   <li>Never splitting tables or lists (atomic units)</li>
 *   <li>Splitting oversized text at sentence boundaries when necessary</li>
 *   <li>Preserving content type metadata for each chunk</li>
 * </ul>
 *
 * <p>This is the standard chunking approach for all text extraction in SEC adapters
 * including MD&A, footnotes, risk factors, and earnings transcripts.
 */
public class SemanticTextChunker {
  private static final Logger LOGGER = LoggerFactory.getLogger(SemanticTextChunker.class);

  // Default chunk size configuration (characters)
  public static final int DEFAULT_TARGET_SIZE = 1000;
  public static final int DEFAULT_MIN_SIZE = 200;
  public static final int DEFAULT_MAX_SIZE = 2000;

  // Sentence boundary pattern - handles common abbreviations
  private static final Pattern SENTENCE_BOUNDARY = Pattern.compile(
      "(?<=[.!?])\\s+(?=[A-Z])|(?<=[.!?])\\s*$");

  // Pattern to detect table of contents entries
  private static final Pattern TOC_PATTERN = Pattern.compile(
      "(?i)^\\s*(item\\s+\\d+|part\\s+[iv]+|table of contents).*\\d+\\s*$");

  // Pattern to detect page numbers and headers
  private static final Pattern PAGE_PATTERN = Pattern.compile(
      "(?i)^\\s*(page\\s*)?\\d+\\s*$|^\\s*-\\s*\\d+\\s*-\\s*$");

  private final int targetSize;
  private final int minSize;
  private final int maxSize;

  /**
   * Content type classifications for chunks.
   */
  public enum ContentType {
    PARAGRAPH,      // Regular text paragraph
    TABLE,          // HTML table (preserved as-is)
    LIST,           // Ordered or unordered list
    HEADING,        // Section heading
    FOOTNOTE,       // Footnote reference or content
    BLOCKQUOTE,     // Quoted content
    MIXED           // Combined from multiple element types
  }

  /**
   * Represents a semantic chunk of text with metadata.
   */
  public static class Chunk {
    private final String text;
    private final ContentType contentType;
    private final int sequenceNumber;
    private final String sourceElements;  // HTML tag names that contributed
    private final List<String> footnoteRefs;

    public Chunk(String text, ContentType contentType, int sequenceNumber,
                 String sourceElements, List<String> footnoteRefs) {
      this.text = text;
      this.contentType = contentType;
      this.sequenceNumber = sequenceNumber;
      this.sourceElements = sourceElements;
      this.footnoteRefs = footnoteRefs != null ? footnoteRefs : new ArrayList<>();
    }

    public String getText() {
      return text;
    }

    public ContentType getContentType() {
      return contentType;
    }

    public int getSequenceNumber() {
      return sequenceNumber;
    }

    public String getSourceElements() {
      return sourceElements;
    }

    public List<String> getFootnoteRefs() {
      return footnoteRefs;
    }

    public int getLength() {
      return text != null ? text.length() : 0;
    }
  }

  /**
   * Creates a chunker with default size settings.
   */
  public SemanticTextChunker() {
    this(DEFAULT_TARGET_SIZE, DEFAULT_MIN_SIZE, DEFAULT_MAX_SIZE);
  }

  /**
   * Creates a chunker with custom size settings.
   *
   * @param targetSize Target chunk size in characters
   * @param minSize    Minimum chunk size (smaller chunks are combined)
   * @param maxSize    Maximum chunk size (larger chunks are split)
   */
  public SemanticTextChunker(int targetSize, int minSize, int maxSize) {
    this.targetSize = targetSize;
    this.minSize = minSize;
    this.maxSize = maxSize;
  }

  /**
   * Chunks an HTML document starting from a specific element.
   *
   * @param startElement The element to start chunking from
   * @param stopPattern  Regex pattern that signals end of section (e.g., "Item 8")
   * @return List of semantic chunks
   */
  public List<Chunk> chunkFromElement(Element startElement, String stopPattern) {
    List<Chunk> chunks = new ArrayList<>();
    if (startElement == null) {
      return chunks;
    }

    Pattern stopRegex = stopPattern != null ? Pattern.compile(stopPattern, Pattern.CASE_INSENSITIVE) : null;
    List<ElementContent> contents = extractElementContents(startElement, stopRegex);
    return combineIntoChunks(contents);
  }

  /**
   * Chunks plain text content, splitting at sentence boundaries.
   *
   * @param text The text to chunk
   * @return List of semantic chunks
   */
  public List<Chunk> chunkPlainText(String text) {
    List<Chunk> chunks = new ArrayList<>();
    if (text == null || text.trim().isEmpty()) {
      return chunks;
    }

    String cleanText = cleanText(text);
    if (cleanText.length() <= maxSize) {
      chunks.add(new Chunk(cleanText, ContentType.PARAGRAPH, 1, "text",
                           extractFootnoteReferences(cleanText)));
      return chunks;
    }

    // Split at sentence boundaries
    List<String> sentences = splitIntoSentences(cleanText);
    StringBuilder currentChunk = new StringBuilder();
    int chunkNum = 1;

    for (String sentence : sentences) {
      if (currentChunk.length() + sentence.length() > targetSize && currentChunk.length() >= minSize) {
        // Emit current chunk
        String chunkText = currentChunk.toString().trim();
        if (!chunkText.isEmpty()) {
          chunks.add(new Chunk(chunkText, ContentType.PARAGRAPH, chunkNum++, "text",
                               extractFootnoteReferences(chunkText)));
        }
        currentChunk = new StringBuilder();
      }
      currentChunk.append(sentence).append(" ");
    }

    // Emit remaining
    String remaining = currentChunk.toString().trim();
    if (!remaining.isEmpty()) {
      chunks.add(new Chunk(remaining, ContentType.PARAGRAPH, chunkNum, "text",
                           extractFootnoteReferences(remaining)));
    }

    return chunks;
  }

  /**
   * Chunks an HTML document section identified by CSS selector.
   *
   * @param doc         The JSoup document
   * @param cssSelector CSS selector for the section
   * @param stopPattern Regex pattern that signals end of section
   * @return List of semantic chunks
   */
  public List<Chunk> chunkSection(Document doc, String cssSelector, String stopPattern) {
    Elements elements = doc.select(cssSelector);
    if (elements.isEmpty()) {
      return new ArrayList<>();
    }
    return chunkFromElement(elements.first(), stopPattern);
  }

  // Internal class to hold element content during extraction
  private static class ElementContent {
    final String text;
    final ContentType type;
    final String tagName;
    final boolean isAtomic;  // Tables and lists should not be split
    final List<String> footnoteRefs;

    ElementContent(String text, ContentType type, String tagName, boolean isAtomic, List<String> footnoteRefs) {
      this.text = text;
      this.type = type;
      this.tagName = tagName;
      this.isAtomic = isAtomic;
      this.footnoteRefs = footnoteRefs;
    }
  }

  /**
   * Extracts content from elements, preserving structure.
   */
  private List<ElementContent> extractElementContents(Element startElement, Pattern stopPattern) {
    List<ElementContent> contents = new ArrayList<>();
    Element current = startElement;

    while (current != null) {
      String text = current.text().trim();

      // Check stop condition
      if (stopPattern != null && stopPattern.matcher(text).find()) {
        break;
      }

      // Skip noise (TOC entries, page numbers)
      if (isNoise(text)) {
        current = current.nextElementSibling();
        continue;
      }

      // Classify and extract content
      ElementContent content = extractContent(current);
      if (content != null && !content.text.isEmpty()) {
        contents.add(content);
      }

      current = current.nextElementSibling();
    }

    return contents;
  }

  /**
   * Extracts content from a single element.
   */
  private ElementContent extractContent(Element element) {
    String tagName = element.tagName().toLowerCase();
    String text;
    ContentType type;
    boolean isAtomic = false;
    List<String> footnoteRefs;

    switch (tagName) {
      case "table":
        // Preserve table structure - extract as text but mark as atomic
        text = extractTableText(element);
        type = ContentType.TABLE;
        isAtomic = true;
        footnoteRefs = extractFootnoteReferences(text);
        break;

      case "ul":
      case "ol":
        // Lists are atomic units
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
        break;

      case "blockquote":
        text = cleanText(element.text());
        type = ContentType.BLOCKQUOTE;
        footnoteRefs = extractFootnoteReferences(text);
        break;

      default:
        // Regular paragraph content
        text = cleanText(element.text());
        type = ContentType.PARAGRAPH;
        footnoteRefs = extractFootnoteReferences(text);
        break;
    }

    if (text == null || text.isEmpty()) {
      return null;
    }

    return new ElementContent(text, type, tagName, isAtomic, footnoteRefs);
  }

  /**
   * Combines extracted content into optimal chunks.
   */
  private List<Chunk> combineIntoChunks(List<ElementContent> contents) {
    List<Chunk> chunks = new ArrayList<>();
    StringBuilder currentChunk = new StringBuilder();
    List<String> currentTags = new ArrayList<>();
    List<String> currentFootnotes = new ArrayList<>();
    ContentType currentType = null;
    int chunkNum = 1;

    for (ElementContent content : contents) {
      // Atomic content (tables, lists) always gets its own chunk
      if (content.isAtomic) {
        // Emit any pending chunk first
        if (currentChunk.length() > 0) {
          chunks.add(createChunk(currentChunk.toString(), currentType, chunkNum++,
                                 currentTags, currentFootnotes));
          currentChunk = new StringBuilder();
          currentTags = new ArrayList<>();
          currentFootnotes = new ArrayList<>();
          currentType = null;
        }

        // Emit atomic content as its own chunk (even if oversized)
        chunks.add(new Chunk(content.text, content.type, chunkNum++,
                             content.tagName, content.footnoteRefs));
        continue;
      }

      // Check if adding this content would exceed max size
      if (currentChunk.length() + content.text.length() > maxSize && currentChunk.length() >= minSize) {
        // Emit current chunk
        chunks.add(createChunk(currentChunk.toString(), currentType, chunkNum++,
                               currentTags, currentFootnotes));
        currentChunk = new StringBuilder();
        currentTags = new ArrayList<>();
        currentFootnotes = new ArrayList<>();
        currentType = null;
      }

      // Handle oversized single content
      if (content.text.length() > maxSize) {
        // Emit any pending content first
        if (currentChunk.length() > 0) {
          chunks.add(createChunk(currentChunk.toString(), currentType, chunkNum++,
                                 currentTags, currentFootnotes));
          currentChunk = new StringBuilder();
          currentTags = new ArrayList<>();
          currentFootnotes = new ArrayList<>();
          currentType = null;
        }

        // Split oversized content at sentence boundaries
        List<Chunk> splitChunks = chunkPlainText(content.text);
        for (Chunk splitChunk : splitChunks) {
          chunks.add(new Chunk(splitChunk.getText(), content.type, chunkNum++,
                               content.tagName, splitChunk.getFootnoteRefs()));
        }
        continue;
      }

      // Add content to current chunk
      if (currentChunk.length() > 0) {
        currentChunk.append("\n\n");
      }
      currentChunk.append(content.text);
      currentTags.add(content.tagName);
      currentFootnotes.addAll(content.footnoteRefs);

      // Track content type (MIXED if combining different types)
      if (currentType == null) {
        currentType = content.type;
      } else if (currentType != content.type) {
        currentType = ContentType.MIXED;
      }
    }

    // Emit final chunk
    if (currentChunk.length() > 0) {
      chunks.add(createChunk(currentChunk.toString(), currentType, chunkNum,
                             currentTags, currentFootnotes));
    }

    return chunks;
  }

  private Chunk createChunk(String text, ContentType type, int seqNum,
                            List<String> tags, List<String> footnotes) {
    String tagStr = String.join(",", tags);
    return new Chunk(text.trim(), type != null ? type : ContentType.PARAGRAPH,
                     seqNum, tagStr, footnotes);
  }

  /**
   * Extracts text from a table, preserving row/cell structure.
   */
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

  /**
   * Extracts text from a list, preserving item structure.
   */
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

  /**
   * Splits text into sentences.
   */
  private List<String> splitIntoSentences(String text) {
    List<String> sentences = new ArrayList<>();
    Matcher matcher = SENTENCE_BOUNDARY.matcher(text);
    int lastEnd = 0;

    while (matcher.find()) {
      String sentence = text.substring(lastEnd, matcher.start() + 1).trim();
      if (!sentence.isEmpty()) {
        sentences.add(sentence);
      }
      lastEnd = matcher.end();
    }

    // Add remaining text
    if (lastEnd < text.length()) {
      String remaining = text.substring(lastEnd).trim();
      if (!remaining.isEmpty()) {
        sentences.add(remaining);
      }
    }

    // If no sentence boundaries found, return whole text
    if (sentences.isEmpty() && !text.trim().isEmpty()) {
      sentences.add(text.trim());
    }

    return sentences;
  }

  /**
   * Cleans text by removing excessive whitespace and HTML artifacts.
   */
  private String cleanText(String text) {
    if (text == null) {
      return "";
    }
    // Normalize whitespace
    text = text.replaceAll("\\s+", " ").trim();
    // Remove HTML entities that weren't decoded
    text = text.replaceAll("&[a-zA-Z]+;", " ");
    text = text.replaceAll("&#\\d+;", " ");
    // Clean up resulting whitespace
    text = text.replaceAll("\\s+", " ").trim();
    return text;
  }

  /**
   * Checks if text is noise (TOC, page numbers, etc.)
   */
  private boolean isNoise(String text) {
    if (text == null || text.isEmpty()) {
      return true;
    }
    return TOC_PATTERN.matcher(text).matches() || PAGE_PATTERN.matcher(text).matches();
  }

  /**
   * Extracts footnote references from text.
   */
  private List<String> extractFootnoteReferences(String text) {
    List<String> refs = new ArrayList<>();
    if (text == null) {
      return refs;
    }

    // Match common footnote patterns: (1), [1], *, †, ‡, etc.
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

  /**
   * Creates a chunker configured for MD&A extraction.
   */
  public static SemanticTextChunker forMDA() {
    return new SemanticTextChunker(1200, 300, 2500);
  }

  /**
   * Creates a chunker configured for footnotes (smaller chunks).
   */
  public static SemanticTextChunker forFootnotes() {
    return new SemanticTextChunker(800, 150, 1500);
  }

  /**
   * Creates a chunker configured for earnings transcripts.
   */
  public static SemanticTextChunker forEarnings() {
    return new SemanticTextChunker(1500, 400, 3000);
  }

  /**
   * Creates a chunker configured for risk factors.
   */
  public static SemanticTextChunker forRiskFactors() {
    return new SemanticTextChunker(1000, 250, 2000);
  }
}
