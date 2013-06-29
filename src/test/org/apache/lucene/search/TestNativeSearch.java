package org.apache.lucene.search;

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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.CountFacetRequest;
import org.apache.lucene.facet.search.DrillDownQuery;
import org.apache.lucene.facet.search.DrillSideways.DrillSidewaysResult;
import org.apache.lucene.facet.search.DrillSideways;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NativeMMapDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestNativeSearch extends LuceneTestCase {

  private void printTopDocs(TopDocs hits) {
    System.out.println("  maxScore=" + hits.getMaxScore() + " totalHits=" + hits.totalHits);
    for(int i=0;i<hits.scoreDocs.length;i++) {
      System.out.println("  hit " + i + ": " + hits.scoreDocs[i]);
    }
  }

  private void assertSameHits(TopDocs expected, TopDocs actual) {
    //System.out.println("expected hits:");
    //printTopDocs(expected);
    //System.out.println("actual hits:");
    //printTopDocs(actual);
    assertEquals(expected.totalHits, actual.totalHits);
    assertEquals(expected.getMaxScore(), actual.getMaxScore(), 0.000001f);
    assertEquals(expected.scoreDocs.length, actual.scoreDocs.length);
    for(int i=0;i<expected.scoreDocs.length;i++) {
      assertEquals("hit " + i, expected.scoreDocs[i].doc, actual.scoreDocs[i].doc);
      // nocommit why not exactly the same?
      //assertEquals("hit " + i, expected.scoreDocs[i].score, actual.scoreDocs[i].score, 0.0f);
      assertEquals("hit " + i, expected.scoreDocs[i].score, actual.scoreDocs[i].score, 0.00001f);
    }
  }

  private void assertSameHits(IndexSearcher s, Query q) throws IOException {

    Query csq;
    if (q instanceof TermQuery || q instanceof BooleanQuery || q instanceof PhraseQuery) {
      csq = new ConstantScoreQuery(q);
    } else {
      csq = null;
    }

    // First with only top 10:
    //System.out.println("TEST: q=" + q + " topN=10");
    TopDocs expected = s.search(q, 10);
    TopDocs actual = NativeSearch.searchNative(s, q, 10);
    assertSameHits(expected, actual);

    if (csq != null) {
      expected = s.search(csq, 10);
      actual = NativeSearch.searchNative(s, csq, 10);
      assertSameHits(expected, actual);
    }
    
    // First with only top 10:
    int maxDoc = s.getIndexReader().maxDoc();
    //System.out.println("TEST: q=" + q + " topN=" + maxDoc);
    expected = s.search(q, maxDoc);
    actual = NativeSearch.searchNative(s, q, maxDoc);
    assertSameHits(expected, actual);

    if (csq != null) {
      expected = s.search(csq, maxDoc);
      actual = NativeSearch.searchNative(s, csq, maxDoc);
      assertSameHits(expected, actual);
    }
  }

  private void assertSameHits(DrillSideways ds, DrillDownQuery q, FacetSearchParams fsp) throws IOException {
    DrillSidewaysResult expected = ds.search(null, q, 10, fsp);
    DrillSidewaysResult actual = NativeSearch.drillSidewaysSearchNative(ds, q, 10, fsp);

    assertSameHits(expected.hits, actual.hits);
    assertEquals(expected.facetResults.size(), actual.facetResults.size());
    for(int i=0;i<expected.facetResults.size();i++) {
      FacetResult frExpected = expected.facetResults.get(i);
      FacetResult frActual = actual.facetResults.get(i);
      assertEquals(toSimpleString(frExpected), toSimpleString(frActual));
    }
  }

  public void testBasicBooleanQuery() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("field", "a b c", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "a b x", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "x", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    w.close();
    IndexSearcher s = new IndexSearcher(r);

    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "c")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "x")), BooleanClause.Occur.SHOULD);

    assertSameHits(s, bq);

    // no matches:
    bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "p")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "p")), BooleanClause.Occur.SHOULD);
    assertSameHits(s, bq);
    
    r.close();
    dir.close();
  }

  public void testBooleanMustNotQuery() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("field", "a b c", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "a b x", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "x", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    w.close();
    IndexSearcher s = new IndexSearcher(r);

    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "x")), BooleanClause.Occur.MUST_NOT);
    assertSameHits(s, bq);
    
    r.close();
    dir.close();
  }

  public void testBooleanShouldMustQuery() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("field", "a b c", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "a b x", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "x", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    w.close();
    IndexSearcher s = new IndexSearcher(r);

    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "x")), BooleanClause.Occur.MUST);
    assertSameHits(s, bq);
    
    r.close();
    dir.close();
  }

  public void testBooleanShouldMustMustNotQuery() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("field", "a b c", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "a b x", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "x", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    w.close();
    IndexSearcher s = new IndexSearcher(r);

    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "x")), BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term("field", "c")), BooleanClause.Occur.MUST_NOT);
    assertSameHits(s, bq);
    
    r.close();
    dir.close();
  }

  public void testBooleanShouldMustMustNotQueryWithDeletes() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("field", "a b c", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "a b x", Field.Store.NO));
    doc.add(new StringField("id", "0", Field.Store.NO));
    w.addDocument(doc);
    w.deleteDocuments(new Term("id", "0"));
    doc = new Document();
    doc.add(new TextField("field", "x", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    w.close();
    IndexSearcher s = new IndexSearcher(r);

    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "x")), BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term("field", "c")), BooleanClause.Occur.MUST_NOT);
    assertSameHits(s, bq);
    
    r.close();
    dir.close();
  }

  public void testBooleanMustQuery() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("field", "a b c", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "a b x", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "x", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    w.close();
    IndexSearcher s = new IndexSearcher(r);

    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term("field", "x")), BooleanClause.Occur.MUST);
    assertSameHits(s, bq);
    
    r.close();
    dir.close();
  }

  public void testBooleanShouldMustQueryWithDeletes() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("field", "a b c", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "a b x", Field.Store.NO));
    doc.add(new StringField("id", "0", Field.Store.NO));
    w.addDocument(doc);
    w.deleteDocuments(new Term("id", "0"));
    doc = new Document();
    doc.add(new TextField("field", "x", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    w.close();
    IndexSearcher s = new IndexSearcher(r);

    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "x")), BooleanClause.Occur.MUST);
    assertSameHits(s, bq);
    
    r.close();
    dir.close();
  }

  public void testBooleanMustQueryWithDeletes() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("field", "a b c", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "a b x", Field.Store.NO));
    doc.add(new StringField("id", "0", Field.Store.NO));
    w.addDocument(doc);
    w.deleteDocuments(new Term("id", "0"));
    doc = new Document();
    doc.add(new TextField("field", "x", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    w.close();
    IndexSearcher s = new IndexSearcher(r);

    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term("field", "x")), BooleanClause.Occur.MUST);
    assertSameHits(s, bq);
    
    r.close();
    dir.close();
  }

  public void testBooleanMustNotQueryDeletes() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("field", "a b c", Field.Store.NO));
    doc.add(new StringField("id", "0", Field.Store.NO));
    w.addDocument(doc);
    w.deleteDocuments(new Term("id", "0"));
    doc = new Document();
    doc.add(new TextField("field", "a b x", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "x", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    w.close();
    IndexSearcher s = new IndexSearcher(r);

    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "x")), BooleanClause.Occur.MUST_NOT);
    assertSameHits(s, bq);
    
    r.close();
    dir.close();
  }

  public void testBasicBooleanQueryWithDeletes() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("field", "a b c", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "a b x", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "x", Field.Store.NO));
    doc.add(new StringField("id", "0", Field.Store.NO));
    w.addDocument(doc);
    w.deleteDocuments(new Term("id", "0"));
    IndexReader r = DirectoryReader.open(w, true);
    w.close();
    IndexSearcher s = new IndexSearcher(r);

    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "c")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "x")), BooleanClause.Occur.SHOULD);

    assertSameHits(s, bq);

    // no matches:
    bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "p")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "p")), BooleanClause.Occur.SHOULD);
    assertSameHits(s, bq);
    
    r.close();
    dir.close();
  }

  public void testBasicBooleanQuery2() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    int numDocs = 140;
    for(int i=0;i<numDocs;i++) {
      Document doc = new Document();
      doc.add(new TextField("field", "a b c", Field.Store.NO));
      w.addDocument(doc);
    }

    IndexReader r = DirectoryReader.open(w, true);
    w.close();

    IndexSearcher s = new IndexSearcher(r);

    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "c")), BooleanClause.Occur.SHOULD);

    assertSameHits(s, bq);

    r.close();
    dir.close();
  }

  public void testVarious() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    int numDocs = atLeast(10000);
    for(int i=0;i<numDocs;i++) {
      Document doc = new Document();
      StringBuilder content = new StringBuilder();
      content.append(" a");
      if ((i % 2) == 0) {
        content.append(" b");
      }
      if ((i % 4) == 0) {
        content.append(" c");
      }
      if ((i % 8) == 0) {
        content.append(" d");
      }
      if ((i % 16) == 0) {
        content.append(" e");
      }
      if ((i % 32) == 0) {
        content.append(" f");
      }
      doc.add(new TextField("field", content.toString(), Field.Store.NO));
      w.addDocument(doc);
    }
    w.commit();
    w.close();

    while(true) {
      
      //System.out.println("\nTEST: open reader");
      DirectoryReader r1 = DirectoryReader.open(dir);
      w = new IndexWriter(dir, iwc);
      w.deleteDocuments(new Term("field", "e"));
      w.close();
      DirectoryReader r2 = DirectoryReader.openIfChanged(r1);
      assertTrue(r2 != null);

      IndexSearcher s1 = new IndexSearcher(r1);
      //System.out.println("seacher=" + s1 + " maxDoc=" + s1.getIndexReader().maxDoc());
      IndexSearcher s2 = new IndexSearcher(r2);

      for(int i=0;i<10000;i++) {
        //if (i % 100 == 0) {
        //System.out.println("  " + i + "...");
        //}

        for(int d=0;d<2;d++) {
          IndexSearcher s = d == 0 ? s1 : s2;
          //System.out.println("TEST: d=" + d);

          assertSameHits(s, new TermQuery(new Term("field", "a")));

          for(int j=1;j<6;j++) {
            //System.out.println("TEST: j=" + j);
            String other = String.valueOf((char) (97+j));
            BooleanQuery bq = new BooleanQuery();
            bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
            bq.add(new TermQuery(new Term("field", other)), BooleanClause.Occur.SHOULD);
            assertSameHits(s, bq);
            assertSameHits(s, new ConstantScoreQuery(bq));

            bq = new BooleanQuery();
            bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
            bq.add(new TermQuery(new Term("field", other)), BooleanClause.Occur.MUST);
            assertSameHits(s, bq);
            assertSameHits(s, new ConstantScoreQuery(bq));

            bq = new BooleanQuery();
            bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.MUST);
            bq.add(new TermQuery(new Term("field", other)), BooleanClause.Occur.SHOULD);
            assertSameHits(s, bq);
            assertSameHits(s, new ConstantScoreQuery(bq));
            bq = new BooleanQuery();
            bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
            bq.add(new TermQuery(new Term("field", other)), BooleanClause.Occur.MUST_NOT);
            assertSameHits(s, bq);
            assertSameHits(s, new ConstantScoreQuery(bq));

            bq = new BooleanQuery();
            bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.MUST_NOT);
            bq.add(new TermQuery(new Term("field", other)), BooleanClause.Occur.SHOULD);
            assertSameHits(s, bq);
            assertSameHits(s, new ConstantScoreQuery(bq));

            assertSameHits(s, new TermQuery(new Term("field", other)));
          }

          assertSameHits(s, new ConstantScoreQuery(new TermQuery(new Term("field", "a"))));
        }
        // comment out to test for mem leaks:
        break;
      }

      //System.out.println("\nTEST: close reader");

      r1.close();
      r2.close();
      // comment out to test for mem leaks:
      break;
    }

    dir.close();
  }

  public void testBasicTermQuery() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("field", "a b c", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "a b x", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "x", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    w.close();
    IndexSearcher s = new IndexSearcher(r);

    assertSameHits(s, new TermQuery(new Term("field", "a")));

    r.close();
    dir.close();
  }

  public void testConstantScoreTermQuery() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("field", "a b c", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "a b x", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "x", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    w.close();
    IndexSearcher s = new IndexSearcher(r);

    assertSameHits(s, new ConstantScoreQuery(new TermQuery(new Term("field", "a"))));

    r.close();
    dir.close();
  }

  public void testConstantScoreBooleanQuery() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("field", "a b c", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "a b x", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "x", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    w.close();
    IndexSearcher s = new IndexSearcher(r);

    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "x")), BooleanClause.Occur.SHOULD);
    assertSameHits(s, new ConstantScoreQuery(bq));

    r.close();
    dir.close();
  }

  public void testTermQueryWithDelete() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("field", "a b c", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "a b x", Field.Store.NO));
    doc.add(new StringField("id", "0", Field.Store.NO));
    w.addDocument(doc);
    w.deleteDocuments(new Term("id", "0"));
    doc = new Document();
    doc.add(new TextField("field", "x", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    w.close();
    IndexSearcher s = new IndexSearcher(r);

    assertSameHits(s, new TermQuery(new Term("field", "x")));

    r.close();
    dir.close();
  }

  public void testPrefixAndWildcardQuery() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    int numDocs = atLeast(10000);
    for(int i=0;i<numDocs;i++) {
      Document doc = new Document();
      String s = null;
      while(true) {
        s = _TestUtil.randomSimpleString(random());
        if (s.length() > 0) {
          break;
        }
      }
      doc.add(new TextField("field", s, Field.Store.NO));
      w.addDocument(doc);
      if (random().nextInt(30) == 17) {
        w.deleteDocuments(new Term("field", s));
      }
    }

    IndexReader r = DirectoryReader.open(w, true);
    w.close();

    IndexSearcher s = new IndexSearcher(r);
    assertSameHits(s, new PrefixQuery(new Term("field", "a")));
    assertSameHits(s, new WildcardQuery(new Term("field", "a*b")));
    r.close();
    dir.close();
  }

  public void testFuzzyQuery() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("field", "lucene", Field.Store.NO));
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    w.close();

    IndexSearcher s = new IndexSearcher(r);
    assertSameHits(s, new FuzzyQuery(new Term("field", "lucne"), 1));
    assertSameHits(s, new FuzzyQuery(new Term("field", "luce"), 2));
    r.close();
    dir.close();
  }

  public void testNumericRangeQuery() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    for(int i=0;i<1717;i++) {
      Document doc = new Document();
      doc.add(new IntField("field", i, Field.Store.NO));
      w.addDocument(doc);
    }

    IndexReader r = DirectoryReader.open(w, true);
    w.close();

    IndexSearcher s = new IndexSearcher(r);
    assertSameHits(s, NumericRangeQuery.newIntRange("field", 17, 1700, true, true));
    r.close();
    dir.close();
  }

  public void testPulsedTermQuery() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("field", "lucene lucene", Field.Store.NO));
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    w.close();

    IndexSearcher s = new IndexSearcher(r);
    assertSameHits(s, new TermQuery(new Term("field", "lucene")));
    r.close();
    dir.close();
  }

  public void testPulsedBooleanQuery() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("field", "lucene lucene", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "fun fun", Field.Store.NO));
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    w.close();

    IndexSearcher s = new IndexSearcher(r);
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "lucene")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "fun")), BooleanClause.Occur.SHOULD);
    assertSameHits(s, bq);
    r.close();
    dir.close();
  }

  public void testExactPhraseQuery() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("field", "the doc", Field.Store.NO));
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    w.close();

    IndexSearcher s = new IndexSearcher(r);
    PhraseQuery q = new PhraseQuery();
    q.add(new Term("field", "the"));
    q.add(new Term("field", "doc"));
    assertSameHits(s, q);
    r.close();
    dir.close();
  }

  public void testExactPhraseQuery2() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("field", "the doc", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "the doc the doc the doc the not doc", Field.Store.NO));
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    w.close();

    IndexSearcher s = new IndexSearcher(r);
    PhraseQuery q = new PhraseQuery();
    q.add(new Term("field", "the"));
    q.add(new Term("field", "doc"));
    assertSameHits(s, q);
    r.close();
    dir.close();
  }

  public void testExactPhraseQuery3() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    int numDocs = atLeast(10000);
    for(int i=0;i<numDocs;i++) {
      Document doc = new Document();
      doc.add(new TextField("field", "abc foo bar the doc x y z", Field.Store.NO));
      w.addDocument(doc);
    }

    IndexReader r = DirectoryReader.open(w, true);
    w.close();

    IndexSearcher s = new IndexSearcher(r);
    PhraseQuery q = new PhraseQuery();
    q.add(new Term("field", "the"));
    q.add(new Term("field", "doc"));
    assertSameHits(s, q);
    r.close();
    dir.close();
  }

  public void testExactPhraseQuery4() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    int every = _TestUtil.nextInt(random(), 2, 1000);
    for(int i=0;i<1100;i++) {
      Document doc = new Document();
      if (i % every == 0) {
        doc.add(new TextField("field", "the doc", Field.Store.NO));
      } else if (i % 2 == 0) {
        doc.add(new TextField("field", "the", Field.Store.NO));
      } else {
        doc.add(new TextField("field", "doc", Field.Store.NO));
      }
      w.addDocument(doc);
    }

    IndexReader r = DirectoryReader.open(w, true);
    w.close();

    IndexSearcher s = new IndexSearcher(r);
    PhraseQuery q = new PhraseQuery();
    q.add(new Term("field", "the"));
    q.add(new Term("field", "doc"));
    assertSameHits(s, q);
    r.close();
    dir.close();
  }

  public void testExactPhraseQuery5() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldType docsOnlyType = new FieldType(TextField.TYPE_NOT_STORED);
    docsOnlyType.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY);
    docsOnlyType.freeze();
    for(int docUpto=0;docUpto<100;docUpto++) {
      //System.out.println("\nTEST: doc=" + docUpto);
      StringBuilder sb = new StringBuilder();
      int numTokens = 10000;
      boolean lastThe = false;
      int matchCount = 0;
      for(int i=0;i<numTokens;i++) {
        switch(random().nextInt(5)) {
        case 0:
          sb.append(" foo");
          lastThe = false;
          break;
        case 1:
          sb.append(" bar");
          lastThe = false;
          break;
        case 2:
          sb.append(" the");
          //System.out.println("the-pos " + i);
          lastThe = true;
          break;
        case 3:
          sb.append(" doc");
          //System.out.println("doc-pos " + i);
          if (lastThe) {
            //System.out.println("match @ pos=" + (i-1));
            matchCount++;
          }
          lastThe = false;
          break;
        case 4:
          sb.append(" baz");
          lastThe = false;
          break;
        }
      }
      //System.out.println("  " + matchCount + " matches");
      Document doc = new Document();
      String sf = sb.toString();
      //System.out.println("field=" + sf);
      doc.add(new TextField("field", sf, Field.Store.NO));
      doc.add(new Field("fieldDocsOnly", sf, docsOnlyType));
      w.addDocument(doc);
    }

    IndexReader r = DirectoryReader.open(w, true);
    w.close();

    IndexSearcher s = new IndexSearcher(r);
    PhraseQuery q = new PhraseQuery();
    q.add(new Term("field", "the"));
    q.add(new Term("field", "doc"));
    assertSameHits(s, q);

    assertSameHits(s, new TermQuery(new Term("field", "foo")));
    assertSameHits(s, new TermQuery(new Term("fieldDocsOnly", "foo")));
    assertSameHits(s, new TermQuery(new Term("field", "bar")));
    assertSameHits(s, new TermQuery(new Term("fieldDocsOnly", "bar")));
    assertSameHits(s, new TermQuery(new Term("field", "the")));
    assertSameHits(s, new TermQuery(new Term("fieldDocsOnly", "the")));
    assertSameHits(s, new TermQuery(new Term("field", "doc")));
    assertSameHits(s, new TermQuery(new Term("fieldDocsOnly", "doc")));

    r.close();
    dir.close();
  }

  private void add(FacetFields facetFields, Document doc, String ... categoryPaths) throws IOException {
    List<CategoryPath> paths = new ArrayList<CategoryPath>();
    for(String categoryPath : categoryPaths) {
      paths.add(new CategoryPath(categoryPath, '/'));
    }
    facetFields.addFields(doc, paths);
  }

  public void testDrillSideways() throws Exception {
    File tmpDir = _TestUtil.getTempDir("nativesearch");
    Directory dir = new NativeMMapDirectory(tmpDir);
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene42"));
    IndexWriter w = new IndexWriter(dir, iwc);

    Directory taxoDir = newDirectory();
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetFields facetFields = new FacetFields(taxoWriter);

    Document doc = new Document();
    doc.add(new TextField("field", "x", Field.Store.NO));
    add(facetFields, doc, "vendor/Intel", "speed/Fast");
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "x y", Field.Store.NO));
    add(facetFields, doc, "vendor/AMD", "speed/Slow");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    w.close();

    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    taxoWriter.close();

    IndexSearcher s = new IndexSearcher(r);

    FacetSearchParams fsp = new FacetSearchParams(
                                new CountFacetRequest(new CategoryPath("vendor"), 10),
                                new CountFacetRequest(new CategoryPath("speed"), 10));

    DrillSideways ds = new DrillSideways(s, taxoReader);
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "x")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "x")), BooleanClause.Occur.SHOULD);

    DrillDownQuery ddq;
    DrillSidewaysResult dsResult;

    ddq = new DrillDownQuery(fsp.indexingParams, bq);
    ddq.add(new CategoryPath("vendor", "Intel"));
    dsResult = NativeSearch.drillSidewaysSearchNative(ds, ddq, 10, fsp);
    assertEquals(1, dsResult.hits.totalHits);
    assertEquals(0, dsResult.hits.scoreDocs[0].doc);
    assertEquals(2, dsResult.facetResults.size());
    assertEquals("vendor (0)\n  AMD (1)\n  Intel (1)\n", toSimpleString(dsResult.facetResults.get(0)));
    assertEquals("speed (0)\n  Fast (1)\n", toSimpleString(dsResult.facetResults.get(1)));

    assertSameHits(ds, ddq, fsp);

    // Multi-select:
    //System.out.println("now test multiselect");
    ddq = new DrillDownQuery(fsp.indexingParams, bq);
    ddq.add(new CategoryPath("vendor", "Intel"), new CategoryPath("vendor", "AMD"));
    //System.out.println("ddq: " + ddq);
    assertSameHits(ds, ddq, fsp);

    dsResult = NativeSearch.drillSidewaysSearchNative(ds, ddq, 10, fsp);
    assertEquals(2, dsResult.hits.totalHits);
    assertEquals(0, dsResult.hits.scoreDocs[0].doc);
    assertEquals(2, dsResult.facetResults.size());
    assertEquals("vendor (0)\n  AMD (1)\n  Intel (1)\n", toSimpleString(dsResult.facetResults.get(0)));
    assertEquals("speed (0)\n  Slow (1)\n  Fast (1)\n", toSimpleString(dsResult.facetResults.get(1)));

    // Two drill-downs:
    //System.out.println("\nTEST: now test double drilldown");
    ddq = new DrillDownQuery(fsp.indexingParams, bq);
    ddq.add(new CategoryPath("vendor", "Intel"));
    ddq.add(new CategoryPath("speed", "Fast"));
    //System.out.println("ddq: " + ddq);
    assertSameHits(ds, ddq, fsp);

    dsResult = NativeSearch.drillSidewaysSearchNative(ds, ddq, 10, fsp);
    assertEquals(1, dsResult.hits.totalHits);
    assertEquals(0, dsResult.hits.scoreDocs[0].doc);
    assertEquals(2, dsResult.facetResults.size());
    assertEquals("vendor (0)\n  Intel (1)\n", toSimpleString(dsResult.facetResults.get(0)));
    assertEquals("speed (0)\n  Fast (1)\n", toSimpleString(dsResult.facetResults.get(1)));

    // SHOULD + MUST_NOT
    //System.out.println("TEST: should + must_not");
    bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "x")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "y")), BooleanClause.Occur.MUST_NOT);
    ddq = new DrillDownQuery(fsp.indexingParams, bq);
    ddq.add(new CategoryPath("vendor", "Intel"));
    //System.out.println("ddq: " + ddq);
    assertSameHits(ds, ddq, fsp);

    dsResult = NativeSearch.drillSidewaysSearchNative(ds, ddq, 10, fsp);
    assertEquals(1, dsResult.hits.totalHits);
    assertEquals(0, dsResult.hits.scoreDocs[0].doc);
    assertEquals(2, dsResult.facetResults.size());
    assertEquals("vendor (0)\n  Intel (1)\n", toSimpleString(dsResult.facetResults.get(0)));
    assertEquals("speed (0)\n  Fast (1)\n", toSimpleString(dsResult.facetResults.get(1)));

    // SHOULD + MUST
    //System.out.println("TEST: should + must_not");
    bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "x")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "y")), BooleanClause.Occur.MUST);
    ddq = new DrillDownQuery(fsp.indexingParams, bq);
    ddq.add(new CategoryPath("vendor", "AMD"));
    assertSameHits(ds, ddq, fsp);

    // SHOULD + MUST + MUST_NOT
    //System.out.println("TEST: should + must_not");
    bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "x")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "x")), BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term("field", "y")), BooleanClause.Occur.MUST_NOT);
    ddq = new DrillDownQuery(fsp.indexingParams, bq);
    ddq.add(new CategoryPath("vendor", "AMD"));
    assertSameHits(ds, ddq, fsp);

    // TermQuery
    Query tq = new TermQuery(new Term("field", "x"));
    ddq = new DrillDownQuery(fsp.indexingParams, tq);
    ddq.add(new CategoryPath("vendor", "AMD"));
    assertSameHits(ds, ddq, fsp);

    taxoReader.close();
    r.close();
    dir.close();
    taxoDir.close();
  }

  // Poached from FacetTestUtils.java:
  private static String toSimpleString(FacetResult fr) {
    StringBuilder sb = new StringBuilder();
    toSimpleString(fr.getFacetRequest().categoryPath.length, 0, sb, fr.getFacetResultNode(), "");
    return sb.toString();
  }
  
  private static void toSimpleString(int startLength, int depth, StringBuilder sb, FacetResultNode node, String indent) {
    sb.append(indent + node.label.components[startLength+depth-1] + " (" + (int) node.value + ")\n");
    for (FacetResultNode childNode : node.subResults) {
      toSimpleString(startLength, depth + 1, sb, childNode, indent + "  ");
    }
  }
}
