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
package org.apache.calcite.adapter.govdata.law;

import org.jsoup.Jsoup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Fetches the docket pages of the decided cases of a Court term from supremecourt.gov.
 *
 * <p>The docket numbers come from the term's "Opinions of the Court" listing
 * ({@link ScotusSlipListing}), so only cases the Court decided are fetched. Each docket page is
 * then read in the layout used for dockets from 2016 onward ({@link ScotusDocketPage}); the
 * older layout is not read.
 *
 * <p>Two kinds of docket are not fetched. A docket the listing gives as {@code 141, Orig.}
 * (original jurisdiction) is addressed on the site by a number the listing does not give. And a
 * consolidated case is fetched under the docket number the listing shows, the lead docket; the
 * companion dockets named in the opinion's footnote are not.
 *
 * <p>Both tables that come from a docket page ({@code scotus_dockets} and
 * {@code scotus_docket_entries}) read through this class, one page at a time.
 */
final class ScotusDocketFetcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScotusDocketFetcher.class);

  private static final String SITE = "https://www.supremecourt.gov";
  private static final String USER_AGENT = "Apache-Calcite-GovData/1.0";
  private static final int TIMEOUT_MS = 120000;

  /** Pause between docket pages, to keep the load on the site modest. */
  private static final long PAUSE_MS = 250;

  /** A docket the site addresses by its number: {@code 23-583} or an application {@code 24A910}. */
  private static final Pattern FETCHABLE = Pattern.compile("^(?:\\d{1,2}-\\d+|\\d{1,2}A\\d+)$");

  private ScotusDocketFetcher() {
  }

  /** True when the docket can be fetched from its number; original-jurisdiction dockets cannot. */
  static boolean fetchable(String docket) {
    return FETCHABLE.matcher(docket).matches();
  }

  /** The address of the docket page, e.g. {@code .../public/24a910.html}. */
  static String pageUrl(String docket) {
    return SITE + "/search.aspx?filename=/docket/docketfiles/html/public/"
        + docket.toLowerCase(Locale.ROOT) + ".html";
  }

  /**
   * The distinct docket numbers of the term's decided cases that can be fetched, in listing order.
   */
  static List<String> docketNumbers(List<ScotusSlipListing.Entry> entries) {
    Set<String> numbers = new LinkedHashSet<String>();
    for (ScotusSlipListing.Entry entry : entries) {
      if (fetchable(entry.docket)) {
        numbers.add(entry.docket);
      }
    }
    return new ArrayList<String>(numbers);
  }

  /** Reads the listing for the term and returns its dockets, fetching each page as it is read. */
  static Iterator<ScotusDocketPage.Docket> forTerm(int term) throws IOException {
    String listingUrl = SITE + "/opinions/slipopinion/" + String.format("%02d", term % 100);
    String html = Jsoup.connect(listingUrl).userAgent(USER_AGENT).timeout(TIMEOUT_MS)
        .maxBodySize(0).execute().body();
    List<ScotusSlipListing.Entry> entries = ScotusSlipListing.parse(html);
    final Deque<String> numbers = new ArrayDeque<String>(docketNumbers(entries));
    LOGGER.info("term {}: {} decided cases, {} dockets to fetch", term, entries.size(),
        numbers.size());

    return new Iterator<ScotusDocketPage.Docket>() {
      private boolean first = true;

      @Override public boolean hasNext() {
        return !numbers.isEmpty();
      }

      @Override public ScotusDocketPage.Docket next() {
        if (numbers.isEmpty()) {
          throw new NoSuchElementException();
        }
        String number = numbers.removeFirst();
        try {
          if (!first) {
            Thread.sleep(PAUSE_MS);
          }
          first = false;
          String url = pageUrl(number);
          String page = Jsoup.connect(url).userAgent(USER_AGENT).timeout(TIMEOUT_MS)
              .maxBodySize(0).execute().body();
          return ScotusDocketPage.parse(page, url);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Interrupted while fetching docket " + number, e);
        }
      }
    };
  }
}
