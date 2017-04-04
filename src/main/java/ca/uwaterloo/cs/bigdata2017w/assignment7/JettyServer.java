/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs.bigdata2017w.assignment7;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfInts;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.ArrayList;
import java.util.NavigableMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.http.HttpStatus;

public class JettyServer extends HttpServlet {
  private Table indexTable;
  private Table collectionTable;
  private Stack<Set<Integer>> stack;

  public JettyServer() {}

  private void initialize(String config, String index, String collection) throws IOException {
    Configuration conf = new Configuration();
    conf.addResource(new Path(config));

    Configuration hbaseConfig = HBaseConfiguration.create(conf);
    Connection connection = ConnectionFactory.createConnection(hbaseConfig);
    indexTable = connection.getTable(TableName.valueOf(index));
    collectionTable = connection.getTable(TableName.valueOf(collection));

    stack = new Stack<>();
  }

  private ArrayList<String> runQuery(String q) throws IOException {
    String[] terms = q.split("\\+");

    for (String t : terms) {
      if (t.equals("AND")) {
        performAND();
      } else if (t.equals("OR")) {
        performOR();
      } else {
        pushTerm(t);
      }
    }

    Set<Integer> set = stack.pop();

    ArrayList<String> coll = new ArrayList<>();
    for (Integer i : set) {
      String line = fetchLine(i);
      String pair = "{\"docid\": " + i + ", \"text\": \"" + line + "\"}";
      coll.add(pair);
    }
    return coll;
  }

  private void pushTerm(String term) throws IOException {
    stack.push(fetchDocumentSet(term));
  }

  private void performAND() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<>();

    for (int n : s1) {
      if (s2.contains(n)) {
        sn.add(n);
      }
    }

    stack.push(sn);
  }

  private void performOR() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<>();

    for (int n : s1) {
      sn.add(n);
    }

    for (int n : s2) {
      sn.add(n);
    }

    stack.push(sn);
  }

  private Set<Integer> fetchDocumentSet(String term) throws IOException {
    Set<Integer> set = new TreeSet<>();

    for (PairOfInts pair : fetchPostings(term)) {
      set.add(pair.getLeftElement());
    }

    return set;
  }

  private ArrayList<PairOfInts> fetchPostings(String term) throws IOException {
    ArrayList<PairOfInts> list = new ArrayList<>();

    Get get = new Get(Bytes.toBytes(term));
    Result result = indexTable.get(get);

    NavigableMap<byte[],byte[]> map = result.getFamilyMap(BuildInvertedIndexHBase.PF);
    for (NavigableMap.Entry<byte[], byte[]> entry : map.entrySet()) {
      list.add(new PairOfInts(Bytes.toInt(entry.getKey()), Bytes.toInt(entry.getValue())));
    }

    return list;
  }

  public String fetchLine(long offset) throws IOException {
    Get get = new Get(Bytes.toBytes(offset));
    Result result = collectionTable.get(get);

    return Bytes.toString(result.getValue(InsertCollectionHBase.PF, InsertCollectionHBase.WORD));
  }

  private static final class Args {
    @Option(name = "-config", metaVar = "[path]", required = true, usage = "config")
    String config;

    @Option(name = "-index", metaVar = "[path]", required = true, usage = "index table")
    String index;

    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection table")
    String collection;

    @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
    String query;
  }

  @Override
  public void init() throws ServletException
  {
    String config = (String) getServletContext().getAttribute("config");
    String index = (String) getServletContext().getAttribute("index");
    String collection = (String) getServletContext().getAttribute("collection");

    try {
      initialize(config, index, collection);
    } catch (IOException e) {
      System.err.println(e.getMessage());
      throw new ServletException("Unable to initialize");
    }
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                   throws ServletException, IOException {
    
    String query = req.getQueryString().substring(6);
    ArrayList<String> items = runQuery(query);

    resp.setStatus(HttpStatus.OK_200);

    resp.getWriter().println("[");
    for (int i = 0; i < items.size(); ++i) {
      if (i == items.size() - 1) {
        resp.getWriter().println(items.get(i));
      } else {
        resp.getWriter().println(items.get(i) + ",");
      }
    }
    resp.getWriter().println("]");
  }
}
