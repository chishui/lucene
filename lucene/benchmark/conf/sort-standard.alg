#/**
# * Licensed to the Apache Software Foundation (ASF) under one or more
# * contributor license agreements.  See the NOTICE file distributed with
# * this work for additional information regarding copyright ownership.
# * The ASF licenses this file to You under the Apache License, Version 2.0
# * (the "License"); you may not use this file except in compliance with
# * the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */
# -------------------------------------------------------------------------------------
# multi val params are iterated by NewRound's, added to reports, start with column name.

merge.factor=mrg:50
compound=false

sort.rng=20000:10000:20000:10000

analyzer=org.apache.lucene.analysis.standard.StandardAnalyzer
directory=FSDirectory
#directory=ByteBuffersDirectory

doc.stored=true
doc.tokenized=true
doc.term.vector=false
log.step=100000

work.dir=data
docs.dir=reuters21578

content.source=org.apache.lucene.benchmark.byTask.feeds.SortableSingleDocSource

query.maker=org.apache.lucene.benchmark.byTask.feeds.SimpleQueryMaker

# task at this depth or less would print when they start
task.max.depth.log=2

log.queries=true
# -------------------------------------------------------------------------------------

{ "Rounds"
  { "Run"
      ResetSystemErase

      { "Populate"
        -CreateIndex
        { "MAddDocs" AddDoc(100) > : 500000
        -ForceMerge(1)
        -CloseIndex
      }

      { "TestSortSpeed"
        OpenReader
        { "LoadFieldCacheAndSearch" SearchWithSort(sort_field:int) > : 1
        { "SearchWithSort" SearchWithSort(sort_field:int) > : 5000
        CloseReader

      }

      NewRound
     } : 4

}

RepSumByName

