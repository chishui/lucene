/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.index;

import static org.apache.lucene.index.IndexWriter.isCongruentSort;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.search.Sort;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/**
 * Holds common state used during segment merging.
 *
 * @lucene.experimental
 */
public class MergeState {

  /** Maps document IDs from old segments to document IDs in the new segment */
  public final DocMap[] docMaps;

  /** {@link SegmentInfo} of the newly merged segment. */
  public final SegmentInfo segmentInfo;

  /** {@link FieldInfos} of the newly merged segment. */
  public FieldInfos mergeFieldInfos;

  /** Stored field producers being merged */
  public final StoredFieldsReader[] storedFieldsReaders;

  /** Term vector producers being merged */
  public final TermVectorsReader[] termVectorsReaders;

  /** Norms producers being merged */
  public final NormsProducer[] normsProducers;

  /** DocValues producers being merged */
  public final DocValuesProducer[] docValuesProducers;

  /** FieldInfos being merged */
  public final FieldInfos[] fieldInfos;

  /** Live docs for each reader */
  public final Bits[] liveDocs;

  /** Postings to merge */
  public final FieldsProducer[] fieldsProducers;

  /** Point readers to merge */
  public final PointsReader[] pointsReaders;

  /** Vector readers to merge */
  public final KnnVectorsReader[] knnVectorsReaders;

  /** Max docs per reader */
  public final int[] maxDocs;

  /** InfoStream for debugging messages. */
  public final InfoStream infoStream;

  /** Executor for intra merge activity */
  public final Executor intraMergeTaskExecutor;

  /** Indicates if the index needs to be sorted * */
  public boolean needsIndexSort;

  /** Returns the document ID maps. */
  public DocMap[] getDocMaps() {
    return docMaps;
  }

  /** Returns the segment info of the newly merged segment. */
  public SegmentInfo getSegmentInfo() {
    return segmentInfo;
  }

  /** Returns the field infos of the newly merged segment. */
  public FieldInfos getMergeFieldInfos() {
    return mergeFieldInfos;
  }

  /** Returns the stored fields readers being merged. */
  public StoredFieldsReader[] getStoredFieldsReaders() {
    return storedFieldsReaders;
  }

  /** Returns the term vectors readers being merged. */
  public TermVectorsReader[] getTermVectorsReaders() {
    return termVectorsReaders;
  }

  /** Returns the norms producers being merged. */
  public NormsProducer[] getNormsProducers() {
    return normsProducers;
  }

  /** Returns the DocValues producers being merged. */
  public DocValuesProducer[] getDocValuesProducers() {
    return docValuesProducers;
  }

  /** Returns the field infos being merged. */
  public FieldInfos[] getFieldInfos() {
    return fieldInfos;
  }

  /** Returns the live docs for each reader. */
  public Bits[] getLiveDocs() {
    return liveDocs;
  }

  /** Returns the postings to merge. */
  public FieldsProducer[] getFieldsProducers() {
    return fieldsProducers;
  }

  /** Returns the point readers to merge. */
  public PointsReader[] getPointsReaders() {
    return pointsReaders;
  }

  /** Returns the vector readers to merge. */
  public KnnVectorsReader[] getKnnVectorsReaders() {
    return knnVectorsReaders;
  }

  /** Returns the max docs per reader. */
  public int[] getMaxDocs() {
    return maxDocs;
  }

  /** Returns the info stream for debugging messages. */
  public InfoStream getInfoStream() {
    return infoStream;
  }

  /** Returns the executor for intra merge activity. */
  public Executor getIntraMergeTaskExecutor() {
    return intraMergeTaskExecutor;
  }

  /** Returns whether the index needs to be sorted. */
  public boolean getNeedsIndexSort() {
    return needsIndexSort;
  }

  MergeState(
      List<CodecReader> readers,
      SegmentInfo segmentInfo,
      InfoStream infoStream,
      Executor intraMergeTaskExecutor)
      throws IOException {
    verifyIndexSort(readers, segmentInfo);
    this.infoStream = infoStream;
    int numReaders = readers.size();
    this.intraMergeTaskExecutor = intraMergeTaskExecutor;

    maxDocs = new int[numReaders];
    fieldsProducers = new FieldsProducer[numReaders];
    normsProducers = new NormsProducer[numReaders];
    storedFieldsReaders = new StoredFieldsReader[numReaders];
    termVectorsReaders = new TermVectorsReader[numReaders];
    docValuesProducers = new DocValuesProducer[numReaders];
    pointsReaders = new PointsReader[numReaders];
    knnVectorsReaders = new KnnVectorsReader[numReaders];
    fieldInfos = new FieldInfos[numReaders];
    liveDocs = new Bits[numReaders];

    int numDocs = 0;
    for (int i = 0; i < numReaders; i++) {
      final CodecReader reader = readers.get(i);

      maxDocs[i] = reader.maxDoc();
      liveDocs[i] = reader.getLiveDocs();
      fieldInfos[i] = reader.getFieldInfos();

      normsProducers[i] = reader.getNormsReader();
      if (normsProducers[i] != null) {
        normsProducers[i] = normsProducers[i].getMergeInstance();
      }

      docValuesProducers[i] = reader.getDocValuesReader();
      if (docValuesProducers[i] != null) {
        docValuesProducers[i] = docValuesProducers[i].getMergeInstance();
      }

      storedFieldsReaders[i] = reader.getFieldsReader();
      if (storedFieldsReaders[i] != null) {
        storedFieldsReaders[i] = storedFieldsReaders[i].getMergeInstance();
      }

      termVectorsReaders[i] = reader.getTermVectorsReader();
      if (termVectorsReaders[i] != null) {
        termVectorsReaders[i] = termVectorsReaders[i].getMergeInstance();
      }

      fieldsProducers[i] = reader.getPostingsReader();
      if (fieldsProducers[i] != null) {
        fieldsProducers[i] = fieldsProducers[i].getMergeInstance();
      }

      pointsReaders[i] = reader.getPointsReader();
      if (pointsReaders[i] != null) {
        pointsReaders[i] = pointsReaders[i].getMergeInstance();
      }

      knnVectorsReaders[i] = reader.getVectorReader();
      if (knnVectorsReaders[i] != null) {
        knnVectorsReaders[i] = knnVectorsReaders[i].getMergeInstance();
      }

      numDocs += reader.numDocs();
    }

    segmentInfo.setMaxDoc(numDocs);

    this.segmentInfo = segmentInfo;
    this.docMaps = buildDocMaps(readers, segmentInfo.getIndexSort());
  }

  // Remap docIDs around deletions
  private DocMap[] buildDeletionDocMaps(List<CodecReader> readers) {

    int totalDocs = 0;
    int numReaders = readers.size();
    DocMap[] docMaps = new DocMap[numReaders];

    for (int i = 0; i < numReaders; i++) {
      LeafReader reader = readers.get(i);
      Bits liveDocs = reader.getLiveDocs();

      final PackedLongValues delDocMap;
      if (liveDocs != null) {
        delDocMap = removeDeletes(reader.maxDoc(), liveDocs);
      } else {
        delDocMap = null;
      }

      final int docBase = totalDocs;
      docMaps[i] =
          docID -> {
            if (liveDocs == null) {
              return docBase + docID;
            } else if (liveDocs.get(docID)) {
              return docBase + (int) delDocMap.get(docID);
            } else {
              return -1;
            }
          };
      totalDocs += reader.numDocs();
    }

    return docMaps;
  }

  private DocMap[] buildDocMaps(List<CodecReader> readers, Sort indexSort) throws IOException {

    if (indexSort == null) {
      // no index sort ... we only must map around deletions, and rebase to the merged segment's
      // docID space
      return buildDeletionDocMaps(readers);
    } else {
      // do a merge sort of the incoming leaves:
      long t0 = System.nanoTime();
      DocMap[] result = MultiSorter.sort(indexSort, readers);
      if (result == null) {
        // already sorted so we can switch back to map around deletions
        return buildDeletionDocMaps(readers);
      } else {
        needsIndexSort = true;
      }
      long t1 = System.nanoTime();
      if (infoStream.isEnabled("SM")) {
        infoStream.message(
            "SM",
            String.format(
                Locale.ROOT,
                "%.2f msec to build merge sorted DocMaps",
                (t1 - t0) / (double) TimeUnit.MILLISECONDS.toNanos(1)));
      }
      return result;
    }
  }

  private static void verifyIndexSort(List<CodecReader> readers, SegmentInfo segmentInfo) {
    Sort indexSort = segmentInfo.getIndexSort();
    if (indexSort == null) {
      return;
    }
    for (CodecReader leaf : readers) {
      Sort segmentSort = leaf.getMetaData().sort();
      if (segmentSort == null || isCongruentSort(indexSort, segmentSort) == false) {
        throw new IllegalArgumentException(
            "index sort mismatch: merged segment has sort="
                + indexSort
                + " but to-be-merged segment has sort="
                + (segmentSort == null ? "null" : segmentSort));
      }
    }
  }

  /** A map of doc IDs. */
  @FunctionalInterface
  public interface DocMap {
    /** Return the mapped docID or -1 if the given doc is not mapped. */
    int get(int docID);
  }

  static PackedLongValues removeDeletes(final int maxDoc, final Bits liveDocs) {
    final PackedLongValues.Builder docMapBuilder =
        PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
    int del = 0;
    for (int i = 0; i < maxDoc; ++i) {
      docMapBuilder.add(i - del);
      if (liveDocs.get(i) == false) {
        ++del;
      }
    }
    return docMapBuilder.build();
  }

  /** Create a new merge instance. */
  public MergeState(
      DocMap[] docMaps,
      SegmentInfo segmentInfo,
      FieldInfos mergeFieldInfos,
      StoredFieldsReader[] storedFieldsReaders,
      TermVectorsReader[] termVectorsReaders,
      NormsProducer[] normsProducers,
      DocValuesProducer[] docValuesProducers,
      FieldInfos[] fieldInfos,
      Bits[] liveDocs,
      FieldsProducer[] fieldsProducers,
      PointsReader[] pointsReaders,
      KnnVectorsReader[] knnVectorsReaders,
      int[] maxDocs,
      InfoStream infoStream,
      Executor intraMergeTaskExecutor,
      boolean needsIndexSort) {
    this.docMaps = docMaps;
    this.segmentInfo = segmentInfo;
    this.mergeFieldInfos = mergeFieldInfos;
    this.storedFieldsReaders = storedFieldsReaders;
    this.termVectorsReaders = termVectorsReaders;
    this.normsProducers = normsProducers;
    this.docValuesProducers = docValuesProducers;
    this.fieldInfos = fieldInfos;
    this.liveDocs = liveDocs;
    this.fieldsProducers = fieldsProducers;
    this.pointsReaders = pointsReaders;
    this.knnVectorsReaders = knnVectorsReaders;
    this.maxDocs = maxDocs;
    this.infoStream = infoStream;
    this.intraMergeTaskExecutor = intraMergeTaskExecutor;
    this.needsIndexSort = needsIndexSort;
  }
}
