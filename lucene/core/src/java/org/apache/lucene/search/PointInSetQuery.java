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
package org.apache.lucene.search;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.PrefixCodedTerms.TermIterator;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ArrayUtil.ByteArrayComparator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Abstract query class to find all documents whose single or multi-dimensional point values,
 * previously indexed with e.g. {@link IntPoint}, is contained in the specified set.
 *
 * <p>This is for subclasses and works on the underlying binary encoding: to create range queries
 * for lucene's standard {@code Point} types, refer to factory methods on those classes, e.g. {@link
 * IntPoint#newSetQuery IntPoint.newSetQuery()} for fields indexed with {@link IntPoint}.
 *
 * @see PointValues
 * @lucene.experimental
 */
public abstract class PointInSetQuery extends Query implements Accountable {
  protected static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(PointInSetQuery.class);

  // A little bit overkill for us, since all of our "terms" are always in the same field:
  final PrefixCodedTerms sortedPackedPoints;
  final int sortedPackedPointsHashCode;
  final String field;
  final int numDims;
  final int bytesPerDim;
  final long ramBytesUsed; // cache
  byte[] lowerPoint = null;
  byte[] upperPoint = null;

  /** Iterator of encoded point values. */
  // TODO: if we want to stream, maybe we should use jdk stream class?
  public abstract static class Stream implements BytesRefIterator {
    @Override
    public abstract BytesRef next();
  }

  /** The {@code packedPoints} iterator must be in sorted order. */
  protected PointInSetQuery(String field, int numDims, int bytesPerDim, Stream packedPoints) {
    this.field = field;
    if (bytesPerDim < 1 || bytesPerDim > PointValues.MAX_NUM_BYTES) {
      throw new IllegalArgumentException(
          "bytesPerDim must be > 0 and <= " + PointValues.MAX_NUM_BYTES + "; got " + bytesPerDim);
    }
    this.bytesPerDim = bytesPerDim;
    if (numDims < 1 || numDims > PointValues.MAX_INDEX_DIMENSIONS) {
      throw new IllegalArgumentException(
          "numDims must be > 0 and <= " + PointValues.MAX_INDEX_DIMENSIONS + "; got " + numDims);
    }

    this.numDims = numDims;

    // In the 1D case this works well (the more points, the more common prefixes they share,
    // typically), but in
    // the > 1 D case, where we are only looking at the first dimension's prefix bytes, it can at
    // worst not hurt:
    PrefixCodedTerms.Builder builder = new PrefixCodedTerms.Builder();
    BytesRefBuilder previous = null;
    BytesRef current;
    while ((current = packedPoints.next()) != null) {
      if (current.length != numDims * bytesPerDim) {
        throw new IllegalArgumentException(
            "packed point length should be "
                + (numDims * bytesPerDim)
                + " but got "
                + current.length
                + "; field=\""
                + field
                + "\" numDims="
                + numDims
                + " bytesPerDim="
                + bytesPerDim);
      }
      if (previous == null) {
        previous = new BytesRefBuilder();
        lowerPoint = new byte[bytesPerDim * numDims];
        assert lowerPoint.length == current.length;
        System.arraycopy(current.bytes, current.offset, lowerPoint, 0, current.length);
      } else {
        int cmp = previous.get().compareTo(current);
        if (cmp == 0) {
          continue; // deduplicate
        } else if (cmp > 0) {
          throw new IllegalArgumentException(
              "values are out of order: saw " + previous + " before " + current);
        }
      }
      builder.add(field, current);
      previous.copyBytes(current);
    }
    sortedPackedPoints = builder.finish();
    sortedPackedPointsHashCode = sortedPackedPoints.hashCode();
    if (previous != null) {
      BytesRef max = previous.get();
      upperPoint = new byte[bytesPerDim * numDims];
      assert upperPoint.length == max.length;
      System.arraycopy(max.bytes, max.offset, upperPoint, 0, max.length);
    }
    ramBytesUsed =
        BASE_RAM_BYTES
            + RamUsageEstimator.sizeOfObject(field)
            + RamUsageEstimator.sizeOfObject(sortedPackedPoints);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public final Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {

    // We don't use RandomAccessWeight here: it's no good to approximate with "match all docs".
    // This is an inverted structure and should be used in the first pass:

    return new ConstantScoreWeight(this, boost) {
      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();

        PointValues values = reader.getPointValues(field);
        if (values == null) {
          // No docs in this segment/field indexed any points
          return null;
        }

        if (values.getNumIndexDimensions() != numDims) {
          throw new IllegalArgumentException(
              "field=\""
                  + field
                  + "\" was indexed with numIndexDims="
                  + values.getNumIndexDimensions()
                  + " but this query has numIndexDims="
                  + numDims);
        }
        if (values.getBytesPerDimension() != bytesPerDim) {
          throw new IllegalArgumentException(
              "field=\""
                  + field
                  + "\" was indexed with bytesPerDim="
                  + values.getBytesPerDimension()
                  + " but this query has bytesPerDim="
                  + bytesPerDim);
        }

        if (values.getDocCount() == 0) {
          return null;
        } else if (lowerPoint != null) {
          assert upperPoint != null;
          ByteArrayComparator comparator = ArrayUtil.getUnsignedComparator(bytesPerDim);
          final byte[] fieldPackedLower = values.getMinPackedValue();
          final byte[] fieldPackedUpper = values.getMaxPackedValue();
          for (int i = 0; i < numDims; ++i) {
            int offset = i * bytesPerDim;
            if (comparator.compare(lowerPoint, offset, fieldPackedUpper, offset) > 0
                || comparator.compare(upperPoint, offset, fieldPackedLower, offset) < 0) {
              return null;
            }
          }
        }

        if (numDims == 1) {
          // We optimize this common case, effectively doing a merge sort of the indexed values vs
          // the queried set:
          return new ScorerSupplier() {
            long cost = -1; // calculate lazily, only once

            @Override
            public Scorer get(long leadCost) throws IOException {
              DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values);
              values.intersect(new MergePointVisitor(sortedPackedPoints.iterator(), result));
              DocIdSetIterator iterator = result.build().iterator();
              return new ConstantScoreScorer(score(), scoreMode, iterator);
            }

            @Override
            public long cost() {
              try {
                if (cost == -1) {
                  // Computing the cost may be expensive, so only do it if necessary
                  DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values);
                  cost =
                      values.estimateDocCount(
                          new MergePointVisitor(sortedPackedPoints.iterator(), result));
                  assert cost >= 0;
                }
                return cost;
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            }
          };
        } else {
          // NOTE: this is naive implementation, where for each point we re-walk the KD tree to
          // intersect.  We could instead do a similar
          // optimization as the 1D case, but I think it'd mean building a query-time KD tree so we
          // could efficiently intersect against the
          // index, which is probably tricky!

          return new ScorerSupplier() {
            long cost = -1; // calculate lazily, only once

            @Override
            public Scorer get(long leadCost) throws IOException {
              DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values);
              SinglePointVisitor visitor = new SinglePointVisitor(result);
              TermIterator iterator = sortedPackedPoints.iterator();
              for (BytesRef point = iterator.next(); point != null; point = iterator.next()) {
                visitor.setPoint(point);
                values.intersect(visitor);
              }
              return new ConstantScoreScorer(score(), scoreMode, result.build().iterator());
            }

            @Override
            public long cost() {
              try {
                if (cost == -1) {
                  DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values);
                  SinglePointVisitor visitor = new SinglePointVisitor(result);
                  TermIterator iterator = sortedPackedPoints.iterator();
                  cost = 0;
                  for (BytesRef point = iterator.next(); point != null; point = iterator.next()) {
                    visitor.setPoint(point);
                    cost += values.estimateDocCount(visitor);
                  }
                  assert cost >= 0;
                }
                return cost;
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            }
          };
        }
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }
    };
  }

  /**
   * Essentially does a merge sort, only collecting hits when the indexed point and query point are
   * the same. This is an optimization, used in the 1D case.
   */
  private class MergePointVisitor implements IntersectVisitor {

    private final DocIdSetBuilder result;
    private final TermIterator iterator;
    private BytesRef nextQueryPoint;
    private final ByteArrayComparator comparator;
    private DocIdSetBuilder.BulkAdder adder;

    public MergePointVisitor(TermIterator iterator, DocIdSetBuilder result) throws IOException {
      this.result = result;
      this.comparator = ArrayUtil.getUnsignedComparator(bytesPerDim);
      this.iterator = iterator;
      nextQueryPoint = iterator.next();
    }

    @Override
    public void grow(int count) {
      adder = result.grow(count);
    }

    @Override
    public void visit(int docID) {
      adder.add(docID);
    }

    @Override
    public void visit(DocIdSetIterator iterator) throws IOException {
      adder.add(iterator);
    }

    @Override
    public void visit(int docID, byte[] packedValue) {
      if (matches(packedValue)) {
        visit(docID);
      }
    }

    @Override
    public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
      if (matches(packedValue)) {
        adder.add(iterator);
      }
    }

    private boolean matches(byte[] packedValue) {
      while (nextQueryPoint != null) {
        int cmp = comparator.compare(nextQueryPoint.bytes, nextQueryPoint.offset, packedValue, 0);
        if (cmp == 0) {
          return true;
        } else if (cmp < 0) {
          // Query point is before index point, so we move to next query point
          nextQueryPoint = iterator.next();
        } else {
          // Query point is after index point, so we don't collect and we return:
          break;
        }
      }
      return false;
    }

    @Override
    public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
      while (nextQueryPoint != null) {
        int cmpMin =
            comparator.compare(nextQueryPoint.bytes, nextQueryPoint.offset, minPackedValue, 0);
        if (cmpMin < 0) {
          // query point is before the start of this cell
          nextQueryPoint = iterator.next();
          continue;
        }
        int cmpMax =
            comparator.compare(nextQueryPoint.bytes, nextQueryPoint.offset, maxPackedValue, 0);
        if (cmpMax > 0) {
          // query point is after the end of this cell
          return Relation.CELL_OUTSIDE_QUERY;
        }

        if (cmpMin == 0 && cmpMax == 0) {
          // NOTE: we only hit this if we are on a cell whose min and max values are exactly equal
          // to our point,
          // which can easily happen if many (> 512) docs share this one value
          return Relation.CELL_INSIDE_QUERY;
        } else {
          return Relation.CELL_CROSSES_QUERY;
        }
      }

      // We exhausted all points in the query:
      return Relation.CELL_OUTSIDE_QUERY;
    }
  }

  /**
   * IntersectVisitor that queries against a highly degenerate shape: a single point. This is used
   * in the > 1D case.
   */
  private class SinglePointVisitor implements IntersectVisitor {

    private final ByteArrayComparator comparator;
    private final DocIdSetBuilder result;
    private final byte[] pointBytes;
    private DocIdSetBuilder.BulkAdder adder;

    public SinglePointVisitor(DocIdSetBuilder result) {
      this.comparator = ArrayUtil.getUnsignedComparator(bytesPerDim);
      this.result = result;
      this.pointBytes = new byte[bytesPerDim * numDims];
    }

    public void setPoint(BytesRef point) {
      // we verified this up front in query's ctor:
      assert point.length == pointBytes.length;
      System.arraycopy(point.bytes, point.offset, pointBytes, 0, pointBytes.length);
    }

    @Override
    public void grow(int count) {
      adder = result.grow(count);
    }

    @Override
    public void visit(int docID) {
      adder.add(docID);
    }

    @Override
    public void visit(DocIdSetIterator iterator) throws IOException {
      adder.add(iterator);
    }

    @Override
    public void visit(int docID, byte[] packedValue) {
      assert packedValue.length == pointBytes.length;
      if (Arrays.equals(packedValue, pointBytes)) {
        // The point for this doc matches the point we are querying on
        visit(docID);
      }
    }

    @Override
    public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
      assert packedValue.length == pointBytes.length;
      if (Arrays.equals(packedValue, pointBytes)) {
        // The point for this set of docs matches the point we are querying on
        adder.add(iterator);
      }
    }

    @Override
    public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {

      boolean crosses = false;

      for (int dim = 0; dim < numDims; dim++) {
        int offset = dim * bytesPerDim;

        int cmpMin = comparator.compare(minPackedValue, offset, pointBytes, offset);
        if (cmpMin > 0) {
          return Relation.CELL_OUTSIDE_QUERY;
        }

        int cmpMax = comparator.compare(maxPackedValue, offset, pointBytes, offset);
        if (cmpMax < 0) {
          return Relation.CELL_OUTSIDE_QUERY;
        }

        if (cmpMin != 0 || cmpMax != 0) {
          crosses = true;
        }
      }

      if (crosses) {
        return Relation.CELL_CROSSES_QUERY;
      } else {
        // NOTE: we only hit this if we are on a cell whose min and max values are exactly equal to
        // our point,
        // which can easily happen if many docs share this one value
        return Relation.CELL_INSIDE_QUERY;
      }
    }
  }

  public Collection<byte[]> getPackedPoints() {
    return new AbstractCollection<>() {

      @Override
      public Iterator<byte[]> iterator() {
        int size = (int) sortedPackedPoints.size();
        PrefixCodedTerms.TermIterator iterator = sortedPackedPoints.iterator();
        return new Iterator<>() {

          int upto = 0;

          @Override
          public boolean hasNext() {
            return upto < size;
          }

          @Override
          public byte[] next() {
            if (upto == size) {
              throw new NoSuchElementException();
            }

            upto++;
            BytesRef next = iterator.next();
            return BytesRef.deepCopyOf(next).bytes;
          }
        };
      }

      @Override
      public int size() {
        return (int) sortedPackedPoints.size();
      }
    };
  }

  public String getField() {
    return field;
  }

  public int getNumDims() {
    return numDims;
  }

  public int getBytesPerDim() {
    return bytesPerDim;
  }

  @Override
  public final int hashCode() {
    int hash = classHash();
    hash = 31 * hash + field.hashCode();
    hash = 31 * hash + sortedPackedPointsHashCode;
    hash = 31 * hash + numDims;
    hash = 31 * hash + bytesPerDim;
    return hash;
  }

  @Override
  public final boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(PointInSetQuery other) {
    return other.field.equals(field)
        && other.numDims == numDims
        && other.bytesPerDim == bytesPerDim
        && other.sortedPackedPointsHashCode == sortedPackedPointsHashCode
        && other.sortedPackedPoints.equals(sortedPackedPoints);
  }

  @Override
  public final String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    if (this.field.equals(field) == false) {
      sb.append(this.field);
      sb.append(':');
    }

    sb.append("{");

    TermIterator iterator = sortedPackedPoints.iterator();
    byte[] pointBytes = new byte[numDims * bytesPerDim];
    boolean first = true;
    for (BytesRef point = iterator.next(); point != null; point = iterator.next()) {
      if (first == false) {
        sb.append(" ");
      }
      first = false;
      System.arraycopy(point.bytes, point.offset, pointBytes, 0, pointBytes.length);
      sb.append(toString(pointBytes));
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Returns a string of a single value in a human-readable format for debugging. This is used by
   * {@link #toString()}.
   *
   * <p>The default implementation encodes the individual byte values.
   *
   * @param value single value, never null
   * @return human readable value for debugging
   */
  protected abstract String toString(byte[] value);

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }
}
