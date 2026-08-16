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
package org.apache.calcite.adapter.file.similarity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Pure-Java BERT WordPiece tokenizer, built from a HuggingFace {@code tokenizer.json}.
 *
 * <p>Exists so the query-time embedder needs no native tokenizer library. The alternatives each
 * drag in platform-specific binaries — DJL's {@code ai.djl.huggingface:tokenizers} ships a native
 * lib per OS/arch, and the Go/hugot route needs {@code libtokenizers.a} plus a cgo cross-compile
 * matrix. WordPiece is small enough to implement directly, and doing so leaves ONNX Runtime as the
 * only native dependency in the embedding path.
 *
 * <p>Implements exactly the pipeline {@code snowflake-arctic-embed-xs} declares in its
 * {@code tokenizer.json}: {@code BertNormalizer} (clean_text, handle_chinese_chars, lowercase, no
 * accent stripping), {@code BertPreTokenizer} (whitespace + punctuation splitting), then greedy
 * longest-match-first WordPiece with the {@code ##} continuation prefix, {@code [UNK]} for
 * unmatchable words and a 100-character per-word ceiling. Sequences are wrapped as
 * {@code [CLS] … [SEP]} and truncated right to 512 tokens.
 *
 * <p>This is deliberately not a general HuggingFace tokenizer: it reads the vocab and the handful
 * of flags this model uses, and throws if handed a {@code tokenizer.json} that asks for anything
 * else — a tokenizer that silently ignored an unsupported normalizer would produce token ids that
 * look plausible and embed into a different vector space than the corpus.
 */
public final class BertWordPieceTokenizer {

  private static final int MAX_INPUT_CHARS_PER_WORD = 100;

  private final Map<String, Integer> vocab;
  private final String unkToken;
  private final String continuingPrefix;
  private final boolean lowercase;
  private final boolean cleanText;
  private final boolean handleChineseChars;
  private final boolean stripAccents;
  private final int clsId;
  private final int sepId;
  private final int maxLength;

  private BertWordPieceTokenizer(Map<String, Integer> vocab, String unkToken,
      String continuingPrefix, boolean lowercase, boolean cleanText, boolean handleChineseChars,
      boolean stripAccents, int clsId, int sepId, int maxLength) {
    this.vocab = vocab;
    this.unkToken = unkToken;
    this.continuingPrefix = continuingPrefix;
    this.lowercase = lowercase;
    this.cleanText = cleanText;
    this.handleChineseChars = handleChineseChars;
    this.stripAccents = stripAccents;
    this.clsId = clsId;
    this.sepId = sepId;
    this.maxLength = maxLength;
  }

  /** One tokenized sequence, in the shape the ONNX model's three inputs expect. */
  public static final class Encoding {
    public final long[] inputIds;
    public final long[] attentionMask;
    public final long[] tokenTypeIds;

    Encoding(long[] inputIds, long[] attentionMask, long[] tokenTypeIds) {
      this.inputIds = inputIds;
      this.attentionMask = attentionMask;
      this.tokenTypeIds = tokenTypeIds;
    }
  }

  /**
   * Parses a HuggingFace {@code tokenizer.json}.
   *
   * @throws IllegalArgumentException when the file declares a model or normalizer this
   *     implementation does not reproduce exactly
   */
  public static BertWordPieceTokenizer fromJson(InputStream in) throws IOException {
    JsonNode root = new ObjectMapper().readTree(in);

    JsonNode model = root.path("model");
    String modelType = model.path("type").asText("");
    if (!"WordPiece".equals(modelType)) {
      throw new IllegalArgumentException(
          "unsupported tokenizer model '" + modelType + "'; this reader implements WordPiece only");
    }

    JsonNode norm = root.path("normalizer");
    String normType = norm.path("type").asText("");
    if (!"BertNormalizer".equals(normType)) {
      throw new IllegalArgumentException(
          "unsupported normalizer '" + normType + "'; this reader implements BertNormalizer only");
    }
    JsonNode pre = root.path("pre_tokenizer");
    String preType = pre.path("type").asText("");
    if (!"BertPreTokenizer".equals(preType)) {
      throw new IllegalArgumentException(
          "unsupported pre_tokenizer '" + preType + "'; this reader implements BertPreTokenizer");
    }

    Map<String, Integer> vocab = new HashMap<>();
    Iterator<Map.Entry<String, JsonNode>> fields = model.path("vocab").fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> e = fields.next();
      vocab.put(e.getKey(), e.getValue().asInt());
    }
    if (vocab.isEmpty()) {
      throw new IllegalArgumentException("tokenizer.json declares an empty vocab");
    }

    String unk = model.path("unk_token").asText("[UNK]");
    String prefix = model.path("continuing_subword_prefix").asText("##");
    boolean lower = norm.path("lowercase").asBoolean(true);
    boolean clean = norm.path("clean_text").asBoolean(true);
    boolean chinese = norm.path("handle_chinese_chars").asBoolean(true);
    // strip_accents is commonly null, which HuggingFace resolves to "follow lowercase".
    JsonNode accents = norm.path("strip_accents");
    boolean strip = accents.isNull() || accents.isMissingNode() ? lower : accents.asBoolean();

    Integer cls = vocab.get("[CLS]");
    Integer sep = vocab.get("[SEP]");
    if (cls == null || sep == null) {
      throw new IllegalArgumentException("vocab is missing [CLS] or [SEP]");
    }
    int max = root.path("truncation").path("max_length").asInt(512);

    return new BertWordPieceTokenizer(vocab, unk, prefix, lower, clean, chinese, strip,
        cls, sep, max);
  }

  /** Tokenizes one string to {@code [CLS] … [SEP]}, truncated right to the model's max length. */
  public Encoding encode(String text) {
    List<Integer> ids = new ArrayList<>();
    ids.add(clsId);
    // -2 leaves room for [CLS] and [SEP]; truncation direction is Right, so stop early.
    int budget = maxLength - 2;
    outer:
    for (String word : preTokenize(normalize(text))) {
      for (int id : wordPiece(word)) {
        if (ids.size() - 1 >= budget) {
          break outer;
        }
        ids.add(id);
      }
    }
    ids.add(sepId);

    int n = ids.size();
    long[] inputIds = new long[n];
    long[] mask = new long[n];
    long[] types = new long[n];
    for (int i = 0; i < n; i++) {
      inputIds[i] = ids.get(i);
      mask[i] = 1L;
      types[i] = 0L;
    }
    return new Encoding(inputIds, mask, types);
  }

  /** BertNormalizer: control-char removal, CJK padding, NFD accent stripping, lowercasing. */
  private String normalize(String text) {
    StringBuilder sb = new StringBuilder(text.length() + 16);
    for (int i = 0; i < text.length(); ) {
      int cp = text.codePointAt(i);
      i += Character.charCount(cp);
      if (cleanText) {
        // \0 and unpaired surrogates are dropped; other control chars become spaces.
        if (cp == 0 || cp == 0xFFFD) {
          continue;
        }
        if (cp != '\t' && cp != '\n' && cp != '\r' && Character.isISOControl(cp)) {
          continue;
        }
        if (Character.isWhitespace(cp)) {
          sb.append(' ');
          continue;
        }
      }
      if (handleChineseChars && isChinese(cp)) {
        sb.append(' ').appendCodePoint(cp).append(' ');
        continue;
      }
      sb.appendCodePoint(cp);
    }
    String out = sb.toString();
    if (lowercase) {
      out = out.toLowerCase(java.util.Locale.ROOT);
    }
    if (stripAccents) {
      out = java.text.Normalizer.normalize(out, java.text.Normalizer.Form.NFD);
      StringBuilder noMarks = new StringBuilder(out.length());
      for (int i = 0; i < out.length(); i++) {
        if (Character.getType(out.charAt(i)) != Character.NON_SPACING_MARK) {
          noMarks.append(out.charAt(i));
        }
      }
      out = noMarks.toString();
    }
    return out;
  }

  /** BertPreTokenizer: split on whitespace, then split each punctuation char into its own token. */
  private static List<String> preTokenize(String text) {
    List<String> out = new ArrayList<>();
    StringBuilder cur = new StringBuilder();
    for (int i = 0; i < text.length(); ) {
      int cp = text.codePointAt(i);
      i += Character.charCount(cp);
      if (Character.isWhitespace(cp)) {
        if (cur.length() > 0) {
          out.add(cur.toString());
          cur.setLength(0);
        }
      } else if (isPunctuation(cp)) {
        if (cur.length() > 0) {
          out.add(cur.toString());
          cur.setLength(0);
        }
        out.add(new String(Character.toChars(cp)));
      } else {
        cur.appendCodePoint(cp);
      }
    }
    if (cur.length() > 0) {
      out.add(cur.toString());
    }
    return out;
  }

  /** Greedy longest-match-first WordPiece over one whitespace/punctuation-delimited word. */
  private List<Integer> wordPiece(String word) {
    List<Integer> out = new ArrayList<>();
    if (word.length() > MAX_INPUT_CHARS_PER_WORD) {
      out.add(vocab.get(unkToken));
      return out;
    }
    int start = 0;
    List<Integer> pieces = new ArrayList<>();
    while (start < word.length()) {
      int end = word.length();
      Integer found = null;
      while (start < end) {
        String piece = start == 0 ? word.substring(start, end)
            : continuingPrefix + word.substring(start, end);
        found = vocab.get(piece);
        if (found != null) {
          break;
        }
        end--;
      }
      if (found == null) {
        // Any unmatchable sub-piece invalidates the whole word, per WordPiece.
        out.add(vocab.get(unkToken));
        return out;
      }
      pieces.add(found);
      start = end;
    }
    out.addAll(pieces);
    return out;
  }

  /** The CJK ranges BertNormalizer pads with spaces so each ideograph becomes its own token. */
  private static boolean isChinese(int cp) {
    return (cp >= 0x4E00 && cp <= 0x9FFF)
        || (cp >= 0x3400 && cp <= 0x4DBF)
        || (cp >= 0x20000 && cp <= 0x2A6DF)
        || (cp >= 0x2A700 && cp <= 0x2B73F)
        || (cp >= 0x2B740 && cp <= 0x2B81F)
        || (cp >= 0x2B820 && cp <= 0x2CEAF)
        || (cp >= 0xF900 && cp <= 0xFAFF)
        || (cp >= 0x2F800 && cp <= 0x2FA1F);
  }

  /** BERT treats all ASCII non-alphanumerics as punctuation, plus every Unicode P* category. */
  private static boolean isPunctuation(int cp) {
    if ((cp >= 33 && cp <= 47) || (cp >= 58 && cp <= 64)
        || (cp >= 91 && cp <= 96) || (cp >= 123 && cp <= 126)) {
      return true;
    }
    int type = Character.getType(cp);
    return type == Character.CONNECTOR_PUNCTUATION
        || type == Character.DASH_PUNCTUATION
        || type == Character.START_PUNCTUATION
        || type == Character.END_PUNCTUATION
        || type == Character.INITIAL_QUOTE_PUNCTUATION
        || type == Character.FINAL_QUOTE_PUNCTUATION
        || type == Character.OTHER_PUNCTUATION;
  }
}
