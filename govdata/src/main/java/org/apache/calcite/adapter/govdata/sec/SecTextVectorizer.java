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

import java.util.*;

/**
 * Converts XBRL documents to tagged text for vectorization.
 *
 * Creates contextual chunks that preserve semantic relationships between:
 * - Financial line items and their explanatory text
 * - Risk factors and their related metrics
 * - Footnotes and the items they reference
 *
 * Example output:
 * [CONTEXT: Revenue] [METRIC] Revenue [VALUE] 394B [PERIOD] FY2023
 * [MD&A] Revenue decreased 3% due to...
 * [FOOTNOTE] We recognize revenue when...
 * [RISK] Currency fluctuations may impact revenue...
 */
public class SecTextVectorizer {

  // Concept groups for contextual chunking
  private static final Map<String, List<String>> CONCEPT_GROUPS = new HashMap<>();
  static {
    // Revenue-related concepts
    CONCEPT_GROUPS.put(
        "Revenue", Arrays.asList(
        "Revenue", "RevenueFromContractWithCustomerExcludingAssessedTax",
        "ProductRevenue", "ServiceRevenue", "NetRevenue", "GrossProfit"));

    // Profitability concepts
    CONCEPT_GROUPS.put(
        "Profitability", Arrays.asList(
        "NetIncomeLoss", "OperatingIncomeLoss", "GrossProfit",
        "EarningsPerShareBasic", "EarningsPerShareDiluted"));

    // Liquidity concepts
    CONCEPT_GROUPS.put(
        "Liquidity", Arrays.asList(
        "CashAndCashEquivalentsAtCarryingValue", "WorkingCapital",
        "OperatingCashFlow", "FreeCashFlow", "CurrentRatio"));

    // Debt & Obligations
    CONCEPT_GROUPS.put(
        "Debt", Arrays.asList(
        "LongTermDebt", "ShortTermBorrowings", "DebtCurrent",
        "InterestExpense", "DebtToEquityRatio"));

    // Assets
    CONCEPT_GROUPS.put(
        "Assets", Arrays.asList(
        "Assets", "CurrentAssets", "PropertyPlantAndEquipmentNet",
        "IntangibleAssetsNetExcludingGoodwill", "Goodwill"));
  }

  /**
   * Constructor with specified embedding dimension and provider configuration.
   * NOTE: Provider configuration ignored - use DuckDB expression columns instead
   */
  public SecTextVectorizer(int embeddingDimension, Map<String, Object> textSimilarityConfig) {
    // Embedding provider removed - use DuckDB quackformers extension via expression columns
  }

  /**
   * Constructor with specified embedding dimension using local provider.
   * NOTE: Local provider removed - use DuckDB expression columns instead
   */
  public SecTextVectorizer(int embeddingDimension) {
    // Embedding provider removed - use DuckDB quackformers extension via expression columns
  }

  /**
   * Default constructor.
   */
  public SecTextVectorizer() {
  }

  private String conceptToKeyword(String concept) {
    // Convert XBRL concept to searchable keyword
    return concept.replaceAll("([A-Z])", " $1")
                  .replaceAll("Loss$", "")
                  .replaceAll("AtCarryingValue$", "")
                  .trim();
  }

  private String formatValue(double value) {
    if (value >= 1_000_000_000) {
      return String.format("%.1fB", value / 1_000_000_000);
    } else if (value >= 1_000_000) {
      return String.format("%.1fM", value / 1_000_000);
    } else {
      return String.format("%.0f", value);
    }
  }

  /**
   * Represents a contextual chunk ready for embedding.
   */
  public static class ContextualChunk {
    public final String context;
    public final String text;
    public final String blobType;  // concept_group, footnote, mda_paragraph
    public final String originalBlobId;  // For traceability
    public final Map<String, Object> metadata;  // Relationships, token info, etc.
    public final double[] embedding;  // The actual vector embedding

    public ContextualChunk(String context, String text) {
      this(context, text, "concept_group", null, new HashMap<>(), null);
    }

    public ContextualChunk(String context, String text, String blobType,
                          String originalBlobId, Map<String, Object> metadata) {
      this(context, text, blobType, originalBlobId, metadata, null);
    }

    public ContextualChunk(String context, String text, String blobType,
                          String originalBlobId, Map<String, Object> metadata,
                          double[] embedding) {
      this.context = context;
      this.text = text;
      this.blobType = blobType;
      this.originalBlobId = originalBlobId;
      this.metadata = metadata != null ? metadata : new HashMap<>();
      this.embedding = embedding;
    }
  }

  /**
   * Simple container for financial facts.
   */
  public static class FinancialFact {
    public final String concept;
    public final double value;
    public final String period;

    public FinancialFact(String concept, double value, String period) {
      this.concept = concept;
      this.value = value;
      this.period = period;
    }
  }

  // ========== New Relationship-Based Vectorization Methods ==========

  /**
   * Container for text blob with metadata.
   */
  public static class TextBlob {
    public final String id;
    public final String type;  // footnote, mda_paragraph
    public final String text;
    public final String parentSection;
    public final String subsection;
    public final Map<String, String> attributes;

    public TextBlob(String id, String type, String text, String parentSection) {
      this(id, type, text, parentSection, null, new HashMap<>());
    }

    public TextBlob(String id, String type, String text, String parentSection,
                   String subsection, Map<String, String> attributes) {
      this.id = id;
      this.type = type;
      this.text = text;
      this.parentSection = parentSection;
      this.subsection = subsection;
      this.attributes = attributes;
    }
  }

  /**
   * Create individual enriched chunks for footnotes and MD&A paragraphs.
   * Each blob is enriched with its relationships before vectorization.
   */
  public List<ContextualChunk> createIndividualChunks(
      List<TextBlob> footnotes,
      List<TextBlob> mdaParagraphs,
      Map<String, List<String>> references,
      Map<String, FinancialFact> facts) {

    return createIndividualChunks(footnotes, mdaParagraphs, new ArrayList<>(), references, facts);
  }

  /**
   * Create individual chunks for footnotes, MD&A paragraphs, and earnings content.
   * This is the enhanced version that includes earnings content.
   *
   * @deprecated Use {@link #createIndividualChunks(List, List, List, Map, Map, String, String)}
   *             which includes filing context for text normalization.
   */
  @Deprecated
  public List<ContextualChunk> createIndividualChunks(
      List<TextBlob> footnotes,
      List<TextBlob> mdaParagraphs,
      List<TextBlob> earningsBlobs,
      Map<String, List<String>> references,
      Map<String, FinancialFact> facts) {
    // Delegate to new method with null filing context (no normalization)
    return createIndividualChunks(footnotes, mdaParagraphs, earningsBlobs,
        references, facts, null, null);
  }

  /**
   * Create individual chunks for footnotes, MD&A paragraphs, and earnings content.
   * Applies text normalization for consistent embeddings.
   *
   * <p>This method enriches text with contextual tags and cross-references, then
   * normalizes temporal and monetary expressions for better embedding quality.
   * Embeddings are NOT generated here - use DuckDB's embed_jina() computed column.
   *
   * @param footnotes      List of footnote text blobs
   * @param mdaParagraphs  List of MD&A paragraph text blobs
   * @param earningsBlobs  List of earnings call text blobs
   * @param references     Map of blob ID to list of referencing blob IDs
   * @param facts          Map of financial facts by concept name
   * @param filingDate     Filing date (YYYY-MM-DD) for resolving relative dates
   * @param periodEnd      Period end date (YYYY-MM-DD) for context
   * @return List of contextual chunks with enriched and normalized text
   */
  public List<ContextualChunk> createIndividualChunks(
      List<TextBlob> footnotes,
      List<TextBlob> mdaParagraphs,
      List<TextBlob> earningsBlobs,
      Map<String, List<String>> references,
      Map<String, FinancialFact> facts,
      String filingDate,
      String periodEnd) {

    List<ContextualChunk> chunks = new ArrayList<>();
    SecTokenManager tokenManager = new SecTokenManager();

    // Create text normalizer for this filing's context
    TextNormalizer normalizer = (filingDate != null)
        ? TextNormalizer.forFiling(filingDate, periodEnd)
        : new TextNormalizer();

    // Vectorize each footnote with its relationships
    for (TextBlob footnote : footnotes) {
      ContextualChunk chunk = vectorizeFootnote(footnote, references,
          mdaParagraphs, facts, tokenManager);
      if (chunk != null) {
        // Apply text normalization and store both original and normalized text
        chunk = applyNormalization(chunk, normalizer);
        chunks.add(chunk);
      }
    }

    // Vectorize each MD&A paragraph with referenced footnotes
    for (TextBlob mdaPara : mdaParagraphs) {
      ContextualChunk chunk = vectorizeMDAParagraph(mdaPara, references,
          footnotes, facts, tokenManager);
      if (chunk != null) {
        chunk = applyNormalization(chunk, normalizer);
        chunks.add(chunk);
      }
    }

    // Vectorize each earnings paragraph with referenced footnotes
    for (TextBlob earningsBlob : earningsBlobs) {
      ContextualChunk chunk = vectorizeEarningsParagraph(earningsBlob, references,
          footnotes, facts, tokenManager);
      if (chunk != null) {
        chunk = applyNormalization(chunk, normalizer);
        chunks.add(chunk);
      }
    }

    return chunks;
  }

  /**
   * Apply text normalization to a chunk's enriched text.
   * Stores original text in metadata and returns chunk with normalized text.
   */
  private ContextualChunk applyNormalization(ContextualChunk chunk, TextNormalizer normalizer) {
    // Store original enriched text in metadata
    Map<String, Object> metadata = new HashMap<>(chunk.metadata);
    metadata.put("original_enriched_text", chunk.text);

    // Normalize the text for better embedding quality
    String normalizedText = normalizer.normalize(chunk.text);

    // Return new chunk with normalized text (no embedding - will be computed by DuckDB)
    return new ContextualChunk(
        chunk.context,
        normalizedText,
        chunk.blobType,
        chunk.originalBlobId,
        metadata,
        null  // No embedding - use DuckDB embed_jina() computed column
    );
  }

  /**
   * Vectorize a footnote with contextual enrichment.
   */
  private ContextualChunk vectorizeFootnote(
      TextBlob footnote,
      Map<String, List<String>> references,
      List<TextBlob> mdaParagraphs,
      Map<String, FinancialFact> facts,
      SecTokenManager tokenManager) {

    StringBuilder enriched = new StringBuilder();
    Map<String, Object> metadata = new HashMap<>();

    // Start with main footnote text
    enriched.append("[MAIN:FOOTNOTE:").append(footnote.id).append("] ");
    enriched.append(footnote.text);

    int tokensUsed = tokenManager.estimateTokens(enriched.toString());
    int remainingBudget = SecTokenManager.MAX_TOKENS - tokensUsed;

    // Add parent section if exists
    if (footnote.parentSection != null && remainingBudget > 100) {
      String parentText = String.format("\n[PARENT:%s]", footnote.parentSection);
      if (tokenManager.estimateTokens(parentText) < remainingBudget * 0.1) {
        enriched.append(parentText);
        remainingBudget -= tokenManager.estimateTokens(parentText);
      }
    }

    // Add references from MD&A
    List<String> referencedBy = references.get(footnote.id);
    if (referencedBy != null && !referencedBy.isEmpty() && remainingBudget > 200) {
      metadata.put("referenced_by", referencedBy);
      enriched.append("\n[REFERENCED_BY:");

      // Add first few referencing paragraphs
      int added = 0;
      for (String refId : referencedBy) {
        if (added >= 3 || remainingBudget < 200) break;

        // Find the MD&A paragraph
        TextBlob refPara = findBlobById(mdaParagraphs, refId);
        if (refPara != null) {
          String excerpt = extractRelevantSentence(refPara.text, footnote.id);
          String refText =
              String.format("\n  %s:%s: %s", refPara.parentSection, refId, excerpt);

          int refTokens = tokenManager.estimateTokens(refText);
          if (refTokens < remainingBudget) {
            enriched.append(refText);
            remainingBudget -= refTokens;
            added++;
          }
        }
      }
      enriched.append("]");
    }

    // Add related financial metrics
    List<String> concepts = extractFinancialConcepts(footnote.text);
    if (!concepts.isEmpty() && remainingBudget > 100) {
      metadata.put("financial_concepts", concepts);
      enriched.append("\n[METRICS] ");
      for (String concept : concepts.subList(0, Math.min(3, concepts.size()))) {
        FinancialFact fact = facts.get(concept);
        if (fact != null) {
          enriched.append(concept).append("=").append(formatValue(fact.value)).append(" ");
        }
      }
    }

    // Add metadata about enrichment
    metadata.put("tokens_used", SecTokenManager.MAX_TOKENS - remainingBudget);
    metadata.put("token_budget", SecTokenManager.MAX_TOKENS);
    metadata.put("parent_section", footnote.parentSection);
    if (footnote.subsection != null) {
      metadata.put("subsection", footnote.subsection);
    }
    enriched.append(
        String.format("\n[ENRICHMENT_META tokens=%d/%d]",
        SecTokenManager.MAX_TOKENS - remainingBudget, SecTokenManager.MAX_TOKENS));

    return new ContextualChunk(
        "footnote_" + footnote.id,
        enriched.toString(),
        "footnote",
        footnote.id,
        metadata);
  }

  /**
   * Vectorize an MD&A paragraph with referenced footnotes.
   */
  private ContextualChunk vectorizeMDAParagraph(
      TextBlob mdaPara,
      Map<String, List<String>> references,
      List<TextBlob> footnotes,
      Map<String, FinancialFact> facts,
      SecTokenManager tokenManager) {

    StringBuilder enriched = new StringBuilder();
    Map<String, Object> metadata = new HashMap<>();

    // Start with main paragraph text
    enriched.append("[MAIN:MDA:").append(mdaPara.id).append("] ");
    enriched.append(mdaPara.text);

    int tokensUsed = tokenManager.estimateTokens(enriched.toString());
    int remainingBudget = SecTokenManager.MAX_TOKENS - tokensUsed;

    // Find and add referenced footnotes
    List<String> footnotesReferenced = extractFootnoteReferences(mdaPara.text);
    if (!footnotesReferenced.isEmpty() && remainingBudget > 300) {
      metadata.put("references_footnotes", footnotesReferenced);
      enriched.append("\n[REFERENCED_FOOTNOTES:");

      for (String fnId : footnotesReferenced) {
        if (remainingBudget < 200) break;

        TextBlob footnote = findBlobById(footnotes, fnId);
        if (footnote != null) {
          // Add first paragraph or up to 500 tokens of footnote
          String excerpt =
              tokenManager.getSmartExcerpt(footnote.text, Math.min(500, remainingBudget - 100));
          String fnText = String.format("\n  %s: %s", fnId, excerpt);

          int fnTokens = tokenManager.estimateTokens(fnText);
          if (fnTokens < remainingBudget) {
            enriched.append(fnText);
            remainingBudget -= fnTokens;
          }
        }
      }
      enriched.append("]");
    }

    // Add related financial metrics
    List<String> concepts = extractFinancialConcepts(mdaPara.text);
    if (!concepts.isEmpty() && remainingBudget > 100) {
      metadata.put("financial_concepts", concepts);
      enriched.append("\n[METRICS] ");
      for (String concept : concepts.subList(0, Math.min(3, concepts.size()))) {
        FinancialFact fact = facts.get(concept);
        if (fact != null) {
          enriched.append(concept).append("=").append(formatValue(fact.value)).append(" ");
        }
      }
    }

    // Structured section metadata — stored as columns, NOT injected into embedding text
    metadata.put("parent_section", mdaPara.parentSection);
    if (mdaPara.subsection != null) {
      metadata.put("subsection", mdaPara.subsection);
    }
    if (mdaPara.attributes != null && mdaPara.attributes.containsKey("section_path")) {
      metadata.put("section_path", mdaPara.attributes.get("section_path"));
    }
    if (mdaPara.attributes != null && mdaPara.attributes.containsKey("paragraph_continuation")) {
      metadata.put("paragraph_continuation",
          Boolean.parseBoolean(mdaPara.attributes.get("paragraph_continuation")));
    }

    // Add metadata
    metadata.put("tokens_used", SecTokenManager.MAX_TOKENS - remainingBudget);
    enriched.append(
        String.format("\n[ENRICHMENT_META tokens=%d/%d]",
        SecTokenManager.MAX_TOKENS - remainingBudget, SecTokenManager.MAX_TOKENS));

    return new ContextualChunk(
        "mda_" + mdaPara.id,
        enriched.toString(),
        "mda_paragraph",
        mdaPara.id,
        metadata);
  }

  /**
   * Vectorize earnings call paragraph with contextual enrichment.
   */
  private ContextualChunk vectorizeEarningsParagraph(
      TextBlob earningsBlob,
      Map<String, List<String>> references,
      List<TextBlob> footnotes,
      Map<String, FinancialFact> facts,
      SecTokenManager tokenManager) {

    StringBuilder enriched = new StringBuilder();
    Map<String, Object> metadata = new HashMap<>();

    // Start with main earnings paragraph text
    enriched.append("[MAIN:EARNINGS:").append(earningsBlob.id).append("] ");
    enriched.append(earningsBlob.text);

    int tokensUsed = tokenManager.estimateTokens(enriched.toString());
    int remainingBudget = SecTokenManager.MAX_TOKENS - tokensUsed;

    // Add earnings call context (speaker info and section)
    if (earningsBlob.attributes != null) {
      StringBuilder contextInfo = new StringBuilder();

      String speakerName = earningsBlob.attributes.get("speaker_name");
      String speakerRole = earningsBlob.attributes.get("speaker_role");
      String sectionType = earningsBlob.attributes.get("section_type");

      contextInfo.append("\n[CONTEXT:");
      if (speakerName != null && !speakerName.isEmpty()) {
        contextInfo.append(" SPEAKER:").append(speakerName);
      }
      if (speakerRole != null && !speakerRole.isEmpty()) {
        contextInfo.append(" ROLE:").append(speakerRole);
      }
      if (sectionType != null && !sectionType.isEmpty()) {
        contextInfo.append(" SECTION:").append(sectionType);
      }
      contextInfo.append("]");

      String context = contextInfo.toString();
      if (tokenManager.estimateTokens(context) < remainingBudget * 0.1) {
        enriched.append(context);
        remainingBudget -= tokenManager.estimateTokens(context);
      }
    }

    // Find and add referenced footnotes (earnings calls may reference financial footnotes)
    List<String> footnotesReferenced = extractFootnoteReferences(earningsBlob.text);
    if (!footnotesReferenced.isEmpty() && remainingBudget > 300) {
      metadata.put("references_footnotes", footnotesReferenced);
      enriched.append("\n[REFERENCED_FOOTNOTES:");

      for (String fnId : footnotesReferenced) {
        if (remainingBudget < 200) break;

        TextBlob footnote = findBlobById(footnotes, fnId);
        if (footnote != null) {
          // Add excerpt of footnote
          String excerpt =
              tokenManager.getSmartExcerpt(footnote.text, Math.min(300, remainingBudget - 100));
          String fnText = String.format("\n  %s: %s", fnId, excerpt);

          int fnTokens = tokenManager.estimateTokens(fnText);
          if (fnTokens < remainingBudget) {
            enriched.append(fnText);
            remainingBudget -= fnTokens;
          }
        }
      }
      enriched.append("]");
    }

    // Add related financial metrics mentioned in earnings call
    List<String> concepts = extractFinancialConcepts(earningsBlob.text);
    if (!concepts.isEmpty() && remainingBudget > 100) {
      metadata.put("financial_concepts", concepts);
      enriched.append("\n[METRICS] ");
      for (String concept : concepts.subList(0, Math.min(3, concepts.size()))) {
        FinancialFact fact = facts.get(concept);
        if (fact != null) {
          enriched.append(concept).append("=").append(formatValue(fact.value)).append(" ");
        }
      }
    }

    // Add metadata
    // Structured section metadata — stored as columns, NOT injected into embedding text
    metadata.put("tokens_used", SecTokenManager.MAX_TOKENS - remainingBudget);
    metadata.put("original_text", earningsBlob.text);
    metadata.put("parent_section", earningsBlob.parentSection);
    if (earningsBlob.subsection != null) {
      metadata.put("subsection", earningsBlob.subsection);
    }

    // Include speaker information in metadata
    if (earningsBlob.attributes != null) {
      String speakerName = earningsBlob.attributes.get("speaker_name");
      String speakerRole = earningsBlob.attributes.get("speaker_role");
      if (speakerName != null) metadata.put("speaker_name", speakerName);
      if (speakerRole != null) metadata.put("speaker_role", speakerRole);
    }

    enriched.append(
        String.format("\n[ENRICHMENT_META tokens=%d/%d]",
        SecTokenManager.MAX_TOKENS - remainingBudget, SecTokenManager.MAX_TOKENS));

    return new ContextualChunk(
        "earnings_" + earningsBlob.id,
        enriched.toString(),
        "earnings_paragraph",
        earningsBlob.id,
        metadata);
  }

  /**
   * Extract financial concepts mentioned in text.
   */
  private List<String> extractFinancialConcepts(String text) {
    List<String> concepts = new ArrayList<>();
    String lowerText = text.toLowerCase();

    // Check against all known concepts
    for (List<String> group : CONCEPT_GROUPS.values()) {
      for (String concept : group) {
        String keyword = conceptToKeyword(concept).toLowerCase();
        if (lowerText.contains(keyword)) {
          concepts.add(concept);
        }
      }
    }

    return concepts;
  }

  /**
   * Extract footnote references from text (e.g., "Note 14", "See Note 2").
   */
  private List<String> extractFootnoteReferences(String text) {
    List<String> references = new ArrayList<>();
    java.util.regex.Pattern pattern =
        java.util.regex.Pattern.compile("(?:Note|Footnote)\\s+(\\d+[A-Za-z]?)", java.util.regex.Pattern.CASE_INSENSITIVE);
    java.util.regex.Matcher matcher = pattern.matcher(text);

    while (matcher.find()) {
      references.add("footnote_" + matcher.group(1));
    }

    return references;
  }

  /**
   * Extract the most relevant sentence mentioning a specific item.
   */
  private String extractRelevantSentence(String text, String itemId) {
    String[] sentences = text.split("(?<=[.!?])\\s+");
    String searchTerm = itemId.replace("footnote_", "Note ");

    for (String sentence : sentences) {
      if (sentence.toLowerCase().contains(searchTerm.toLowerCase())) {
        return sentence.length() > 200 ?
            sentence.substring(0, 197) + "..." : sentence;
      }
    }

    // Return first sentence if no specific reference found
    return sentences.length > 0 ?
        (sentences[0].length() > 200 ? sentences[0].substring(0, 197) + "..." : sentences[0])
        : text.substring(0, Math.min(200, text.length()));
  }

  /**
   * Find a text blob by ID.
   */
  private TextBlob findBlobById(List<TextBlob> blobs, String id) {
    for (TextBlob blob : blobs) {
      if (blob.id.equals(id)) {
        return blob;
      }
    }
    return null;
  }
}
